from confluent_kafka import avro


value_schema_str = """
{
   "namespace": "cch.game.event",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "game",
       "type" : "string"
     },
     {
       "name" : "action",
       "type" : "string"
     },
     {
       "name" : "customerID",
       "type" : "string"
     },
     {
       "name" : "stake",
       "type" : "int"
     }
   ]
}
"""

key_schema_str = """
{
   "namespace": "cch.game.event",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "customerID",
       "type" : "string"
     }
   ]
}
"""

game_key_schema = avro.loads(key_schema_str)
game_value_schema = avro.loads(value_schema_str)

