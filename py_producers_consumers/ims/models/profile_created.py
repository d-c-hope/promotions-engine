from confluent_kafka import avro


value_schema_str = """
{
   "namespace": "cch.profile",
   "name": "created",
   "type": "record",
   "fields" : [
     {
       "name" : "customerID",
       "type" : "string"
     },
     {
       "name" : "firstName",
       "type" : "string"
     },
     {
       "name" : "email",
       "type" : "string"
     }
   ]
}
"""

profile_value_schema = avro.loads(value_schema_str)