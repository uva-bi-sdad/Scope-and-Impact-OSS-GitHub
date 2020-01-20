const TASK_PATH = realpath(joinpath("."))

using Pkg
Pkg.activate(TASK_PATH)
Pkg.resolve()
Pkg.instantiate()

using HTTP: Response
using ConfParser: ConfParse, parse_conf!, retrieve
using Dates: unix2datetime, DateTime, now, canonicalize, CompoundPeriod, DateFormat, format
using Diana: Client, GraphQLClient
using JSON3: JSON3
using TimeZones: ZonedDateTime, TimeZone
using LibPQ: Connection, execute, prepare
using Parameters: @unpack
import Base: isless, show

conf = ConfParse(joinpath(TASK_PATH, "confs", "config.simple"));
parse_conf!(conf)
# get and store config parameters
const db_usr = retrieve(conf, "db_usr");
const db_pwd = retrieve(conf, "db_pwd");
const gh_usr = retrieve(conf, "gh_usr");
const db_host = "postgis1"
const db_port = 5432
const dbname = "sdad";
"""
    response_dtf = dateformat"d u y H:M:S Z"
HTTP responses require this datetime format.
"""
const response_dtf = DateFormat("d u y H:M:S Z");
const github_dtf = "yyyy-mm-ddTHH:MM:SSzzzz";
const github_endpoint = "https://api.github.com/graphql";
"""
    until::ZonedDateTime
Until when should the scrapper query data. Currently at `"2019-08-15T00:00:00-04:00"`.
"""
const until = ZonedDateTime("2020-01-01T00:00:00-00:00",
                            github_dtf)

db_host
dbconnect() = Connection("""host = $db_host
                            port = $db_port
                            dbname = $dbname
                            user = $db_usr
                            password = $db_pwd
                         """)

conn = dbconnect()

login_token = collect((row.login, row.pat) for row ∈ execute(conn, """SELECT login, pat FROM gh.pat;"""));

licenses = getproperty.(execute(conn, """SELECT spdx FROM gh.licenses ORDER BY spdx;"""), :spdx);

mutable struct GitHubPersonalAccessToken
    client::Client
    id::String
    remaining::Int
    reset::ZonedDateTime
    function GitHubPersonalAccessToken(login::AbstractString, token::AbstractString)
        client = GraphQLClient(github_endpoint,
                               auth = "bearer $token",
                               headers = Dict("User-Agent" => login))
        result = client.Query(find_repos_by_license, operationName = "")
        remaining = parse(Int, result.Info["X-RateLimit-Remaining"])
        reset = parse(Int, result.Info["X-RateLimit-Reset"]) |>
            unix2datetime |>
            (dt -> ZonedDateTime(dt, TimeZone("UTC")))
        new(client, login, remaining, reset)
    end
end
function show(io::IO, obj::GitHubPersonalAccessToken)
    println(io, "$(obj.id): (remaining: $(obj.remaining))")
end
function isless(x::GitHubPersonalAccessToken, y::GitHubPersonalAccessToken)
    if iszero(x.remaining) && !iszero(y.remaining)
        false
    else
        isless(x.reset, y.reset)
    end
end
function update!(obj::GitHubPersonalAccessToken)
    if obj.reset ≤ now(TimeZone("UTC"))
        obj.remaining = 5_000
    end
    obj
end

"""
    find_repos_by_license::String
Queries for finding open-sourced projects and their commit information from GitHub.
"""
const find_repos_by_license = """
    query Search(\$license_created: String!) {
      search(query: \$license_created,
             type: REPOSITORY,
             first: 100) {
        ...SearchLogic
      }
    }
    query SearchCursor(\$license_created: String!,
                 \$cursor: String!) {
      search(query: \$license_created,
             type: REPOSITORY,
             first: 100,
             after: \$cursor) {
        ...SearchLogic
      }
    }
    fragment SearchLogic on SearchResultItemConnection {
      repositoryCount
      pageInfo {
        endCursor
        hasNextPage
      }
      nodes {
        ... on Repository {
          databaseId
          nameWithOwner
          createdAt
        }
      }
    }
    """;

    """
    binary_search_dt_interval(license::AbstractString,
                              interval::AbstractString)::data, as_of, created_at
Given a license and a datetime interval, it will use binary search to find
a datetime interval with no more than 1,000 results.
"""
@inline function binary_search_dt_interval(pat::AbstractVector{<:GitHubPersonalAccessToken},
                                           license::AbstractString,
                                           created_at::AbstractString)
    dt_start = match(r".*(?=\.{2})", created_at)
    if isnothing(dt_start)
        dt_start = replace(created_at, r"Z$" => "+00:00") |>
            (dt -> ZonedDateTime(dt, github_dtf))
    else
        dt_start = match(r".*(?=\.\.)", created_at).match |>
            (dt -> replace(dt, r"Z$" => "+00:00")) |>
            (dt -> ZonedDateTime(dt, github_dtf))
    end
    dt_end = match(r"(?<=\.{2}).*", created_at)
    if isnothing(dt_end)
        dt_end = until
    else
        dt_end = match(r"(?<=\.{2}).*", created_at).match |>
            (dt -> replace(dt, r"Z$" => "+00:00")) |>
            (dt -> ZonedDateTime(dt, github_dtf))
    end
    foreach(update!, pat)
    sort!(pat)
    next_available = first(pat)
    result = next_available.client.Query(find_repos_by_license,
                                         operationName = "Search",
                                         vars = Dict("license_created" =>
                                                     """license:$license
                                                        archived:false
                                                        fork:false
                                                        mirror:false
                                                        created:$dt_start..$dt_end
                                                     """))
    as_of = get_as_of(result.Info)
    json = JSON3.read(result.Data)
    @assert(haskey(json, :data))
    data = json.data
    repositoryCount = data.search.repositoryCount
    while repositoryCount > 1_000
        dt_end = dt_start + (dt_end - dt_start) ÷ 2 |>
            (dt -> format(dt, github_dtf)) |>
            (dt -> ZonedDateTime(dt, github_dtf))
        foreach(update!, pat)
        sort!(pat)
        next_available = first(pat)
        result = next_available.client.Query(find_repos_by_license,
                                             operationName = "Search",
                                             vars = Dict("license_created" =>
                                                         """license:$license
                                                            archived:false
                                                            fork:false
                                                            mirror:false
                                                            created:$dt_start..$dt_end
                                                          """))
        as_of = get_as_of(result.Info)
        json = JSON3.read(result.Data)
        @assert(haskey(json, :data))
        data = json.data
        repositoryCount = data.search.repositoryCount
    end
    data.search, as_of, "$dt_start..$dt_end"
end
"""
    get_as_of(response::Response)::String
Returns the zoned date time when the response was returned.
"""
get_as_of(response::Response) =
    response.headers[findfirst(x -> isequal("Date", x.first),
                               response.headers)].second[6:end] |>
        (dt -> ZonedDateTime(dt, response_dtf)) |>
        string

github_tokens = [ GitHubPersonalAccessToken(login, token) for (login, token) ∈ login_token ];

insert_spdx_queries = prepare(conn, """INSERT INTO gh.spdx_queries VALUES(\$1, \$2, \$3, \$4);""")
insert_repos = prepare(conn, """INSERT INTO gh.repos VALUES(\$1, \$2, \$3, \$4, \$5, \$6);""")

function find_repos_by_spdx(spdx::AbstractString)
    try
        data, as_of, created_at = binary_search_dt_interval(github_tokens, spdx, "2007-10-29T14:37:16Z..2019-01-01T00:00:00Z")
        execute(insert_spdx_queries, (spdx, created_at, data.repositoryCount, "In Progress"))
        foreach(node -> execute(insert_repos, (node.databaseId, node.nameWithOwner, spdx, node.createdAt, as_of, created_at)), data.nodes)
        while data.pageInfo.hasNextPage
            foreach(update!, github_tokens)
            sort!(github_tokens)
            next_available = first(github_tokens)
            result = next_available.client.Query(find_repos_by_license,
                                                 operationName = "SearchCursor",
                                                 vars = Dict("license_created" =>
                                                             """license:$spdx
                                                                archived:false
                                                                fork:false
                                                                mirror:false
                                                                created:$created_at
                                                             """,
                                                             "cursor" => data.pageInfo.endCursor))
            as_of = get_as_of(result.Info)
            json = JSON3.read(result.Data)
            @assert(haskey(json, :data))
            data = json.data.search
            foreach(node -> execute(insert_repos, (node.databaseId, node.nameWithOwner, spdx, node.createdAt, as_of, created_at)), data.nodes)
        end
        (getproperty.(execute(conn, "SELECT COUNT(*) FROM gh.repos WHERE spdx = '$spdx' AND query = '$created_at';"), :count)[1] ==
         getproperty.(execute(conn, "SELECT count FROM gh.spdx_queries WHERE spdx = '$spdx' AND interval = '$created_at';"), :count)[1]) &&
            execute(conn, "UPDATE gh.spdx_queries SET status = 'done' WHERE spdx = '$spdx' AND interval = '$created_at';")
        while !endswith(created_at, "..2019-01-01T00:00:00+00:00")
            from_dt = match(r"(?<=\.\.).*", created_at).match
            data, as_of, created_at = binary_search_dt_interval(github_tokens, spdx, "$from_dt..2019-01-01T00:00:00Z")
            execute(insert_spdx_queries, (spdx, created_at, data.repositoryCount, "In Progress"))
            foreach(node -> execute(insert_repos, (node.databaseId, node.nameWithOwner, spdx, node.createdAt, as_of, created_at)), data.nodes)
            while data.pageInfo.hasNextPage
                foreach(update!, github_tokens)
                sort!(github_tokens)
                next_available = first(github_tokens)
                result = next_available.client.Query(find_repos_by_license,
                                                     operationName = "SearchCursor",
                                                     vars = Dict("license_created" =>
                                                                 """license:$spdx
                                                                    archived:false
                                                                    fork:false
                                                                    mirror:false
                                                                    created:$created_at
                                                                 """,
                                                                 "cursor" => data.pageInfo.endCursor))
                as_of = get_as_of(result.Info)
                json = JSON3.read(result.Data)
                @assert(haskey(json, :data))
                data = json.data.search
                foreach(node -> execute(insert_repos, (node.databaseId, node.nameWithOwner, spdx, node.createdAt, as_of, created_at)), data.nodes)
            end
            (getproperty.(execute(conn, "SELECT COUNT(*) FROM gh.repos WHERE spdx = '$spdx' AND query = '$created_at';"), :count)[1] ==
             getproperty.(execute(conn, "SELECT count FROM gh.spdx_queries WHERE spdx = '$spdx' AND interval = '$created_at';"), :count)[1]) &&
                execute(conn, "UPDATE gh.spdx_queries SET status = 'done' WHERE spdx = '$spdx' AND interval = '$created_at';")
        end
    catch
        execute(conn, "UPDATE gh.spdx_queries SET status = 'error' WHERE spdx = '$spdx' AND interval = '$created_at';")
    end
end

foreach(find_repos_by_spdx, licenses)
