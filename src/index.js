/**
 * saguin - a JavaScript client library for interacting with the Saguin
 * MQTT 5 broker, built as a thin layer over MQTT.js.
 *
 * `saguin.Client` holds an MQTT.js client and hands it to you as
 * `client.mqtt`, so anything MQTT.js does it does. What this adds sits
 * beside that, named by channel rather than by topic.
 */

export { ChannelInfo, KeyDoesNotFit, compose } from './channels.js'
export {
  DECLARATION, MAX_PARTITIONS, declarations, expand, inside, partition, subscriptions,
  topicHash,
} from './channels.js'
export { DeadLetter, Headers, Message, RESERVED_PREFIX } from './message.js'
export { Reader } from './reader.js'
export {
  AVRO_TYPES, PROTOBUF_TYPES, SCHEMA_PROPERTY, SCHEMA_TYPES, SchemaError, deserialize,
  formatOf, serialize,
} from './schemas.js'
export { Admin, Append, Latest, Queue } from './verbs.js'
export {
  Client,
  ConnectRefused,
  DEFAULT_SESSION_EXPIRY,
  PublishResult,
  RequestRefused,
  SubscriptionRefused,
  UnknownChannel,
  WrongChannelType,
  newMessageId,
} from './client.js'
