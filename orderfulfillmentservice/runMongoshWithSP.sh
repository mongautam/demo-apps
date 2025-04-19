#!/bin/bash

mongosh "$ATLAS_STREAM_PROCESSOR_URL" --tls --authenticationDatabase admin --username $MONGO_USER --password $MONGO_PASS