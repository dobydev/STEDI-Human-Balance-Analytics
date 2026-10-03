CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
    sensorreadingtime BIGINT,
    serialnumber STRING,
    distancefromobject DOUBLE
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
LOCATION
    's3://stedi-human-balance-gdoby/step_trainer_landing/'
TBLPROPERTIES (
    'classification' = 'json'
);