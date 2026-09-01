import strutils
import pkg/[
  chronicles,
  chronicles/helpers,
  chronicles/topics_registry,
]

## Helper for setting log levels which supports the format `level1;level2:topic1,topic2;...`.
## Example:
##    updateLogLevel("INFO;trace:mix_transport)
proc updateLogLevel*(logLevel: string) {.raises: [ValueError].} =
  let directives = logLevel.split(";")
  try:
    setLogLevel(parseEnum[LogLevel](directives[0].toUpperAscii))
  except ValueError:
    raise (ref ValueError)(
      msg:
        "Please specify one of: trace, debug, " & "info, notice, warn, error or fatal"
    )

  if directives.len > 1:
    for topicName, settings in parseTopicDirectives(directives[1 ..^ 1]):
      if not setTopicState(topicName, settings.state, settings.logLevel):
        warn "Unrecognized logging topic", topic = topicName