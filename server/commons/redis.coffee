config = require '../../server_config'
log = require 'winston'
redis = require 'redis'

class Redis
  constructor: ->
    redisConfigurationObject =
      enable_offline_queue: false
      socket_nodelay:true
    if config.redis.password then redisConfigurationObject['auth_pass'] = config.redis.password

    @client = redis.createClient config.redis.port, config.redis.host, redisConfigurationObject
    @setupClientListeners()

  setupClientListeners: ->
    @client.on "error", (err) ->
      log.error "REDIS ERROR: #{err}"
    @client.on "ready",  ->
      log.info "Redis is connected!"

  generateSortedSet: (key) ->
    new RedisSortedSet @client, key

  generateHashClient: ->
    new RedisHashClient @client

module.exports = new Redis()

class RedisHashClient
  constructor: (@client) ->

  fieldExists: (key, field, callback) ->
    @client.hexists key, field, (err, result) ->
      if err? then return callback err, result
      callback err, Boolean(result)

  getField: (key, field, callback) ->
    @client.hget key, field, callback

  setField: (key, field, value, callback) ->
    @client.hset key, field, value, callback



class RedisSortedSet
  constructor: (@client, @key) ->
    log.info "Generated sorted set #{@key}"

  checkIfMemberExists: (member, callback) ->
    @client.zscore @key, member, (error, score) -> callback error, Boolean(score)

  addOrChangeMember: (score, member, callback) ->
    @client.zadd @key, score, member, callback

  getRankOfMember: (member, callback) ->
    @client.zrevrank @key, member, callback

  getScoreOfMember: (member, callback) ->
    @client.zscore @key, member, (error, score) ->
      if error?
        errorObject = "error":error
        return callback errorObject, score

      unless score?
        errorObject = "error":"That element doesn't exist in the sorted set"
        return callback errorObject, score

      callback error, parseFloat(score)

  getAmountOfMembersInSortedSet: (callback) -> @client.zcard @key, callback

  addOrChangeMultipleMembers: (scoreAndMemberObjectArray, callback) ->
    commands = []
    for scoreAndMemberObject in scoreAndMemberObjectArray
      commands.push scoreAndMemberObject.score
      commands.push scoreAndMemberObject.member

    @client.zadd @key, commands, callback

  getAmountOfMembersInScoreRange: (minScore, maxScore, callback) ->
    @client.zcount @key, minScore, maxScore, callback

  getMembersInRankRange: (minRank, maxRank, callback) ->
    @client.zrevrange @key, minRank, maxRank, callback

  getMembersInRankRangeWithScore: (minRank, maxRank, callback) ->
    @client.zrevrange @key, minRank, maxRank, "WITHSCORES", (error, resultList) =>
      @parseResultListAndExecuteCallback error, resultList, callback

  getMembersInScoreRange: (minScore, maxScore, callback) ->
    @client.zrevrangebyscore @key, minScore, maxScore, callback

  getMembersInScoreRangeWithScore: (minScore, maxScore, callback) ->
    @client.zrevrangebyscore @key, minScore, maxScore, "WITHSCORES", (error, resultList) =>
      @parseResultListAndExecuteCallback error, resultList, callback

  parseResultListAndExecuteCallback: (error, resultList, callback) ->
    if err? then return callback error, resultList
    unless resultList then callback error, resultList

    resultArray = []
    elementIndices = (index for index in [0...resultList.length] by 2)

    for elementIndex in elementIndices
      elementObject =
        member: resultList[elementIndex]
        score: resultList[elementIndex + 1]
      resultArray.push elementObject
    callback error, resultArray







