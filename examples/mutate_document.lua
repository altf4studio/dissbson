-- This script will mutate the document by adding the field "_id_hash"

doc["_id_hash"] = seaHash(doc["_id"]["stringRepr"])
