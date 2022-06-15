const { Stack, Duration, RemovalPolicy, Aws } = require('aws-cdk-lib');
const { Bucket } = require('aws-cdk-lib/aws-s3');
const { Database } = require("@aws-cdk/aws-glue-alpha");
const { CfnTable } = require('aws-cdk-lib/aws-glue');

class GlueStack extends Stack {
    /**
   *
   * @param {Construct} scope
   * @param {string} id
   * @param {StackProps=} props
   */
  constructor(scope, id, props) {
    super(scope, id, props);

    this.glueDb = Database.fromDatabaseArn(this, "athenaDefaultDb", "arn:aws:athena:eu-central-1::workgroup/default");

    this.glueTable = new CfnTable(this, "table_glue", {
        databaseName: this.glueDb.databaseName,
        catalogId: Aws.ACCOUNT_ID,
        tableInput: {
            name: process.env.glueTableName,
            tableType: "EXTERNAL_TABLE",
            partitionKeys: [
                {
                    "name": "year",
                    "type": "string"
                },
                {
                    "name": "month",
                    "type": "string"
                }
            ],
            parameters: {
                // "projection.datehour.format": "yyyy/mm/dd/hh",
                // "projection.datehour.interval": "1",
                // "projection.datehour.interval.unit": "HOURS",
                "projection.year.type": "injected",
                "projection.month.type": "injected",
                "projection.enabled": "true",
                "storage.location.template": `s3://${process.env.dataLakeName}` + '/lake/${year}/${month}'
            },
            storageDescriptor: {
                columns: [
                    {
                        "name": "InvoiceNo",
                        "type": "string"
                    },
                    {
                        "name": "StockCode",
                        "type": "string"
                    },
                    {
                        "name": "Description",
                        "type": "string"
                    },
                    {
                        "name": "Quantity",
                        "type": "double"
                    },
                    {
                        "name": "InvoiceDate",
                        "type": "string"
                    },
                    {
                        "name": "UnitPrice",
                        "type": "double"
                    },
                    {
                        "name": "CustomerId",
                        "type": "string"
                    },
                    {
                        "name": "Country",
                        "type": "string"
                    }
                ],
                parameters: {
                    "serialization.format": 1
                },
                serdeInfo: {
                    serializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
                location: `s3://${props.bucket.bucketName}/lake/`,
                inputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                outputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
            }
        }
    })

  }
}

module.exports = {
    GlueStack
}