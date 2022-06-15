const { Stack, Duration, RemovalPolicy, Aws } = require('aws-cdk-lib');
const { PropagatedTagSource } = require('aws-cdk-lib/aws-ecs');
const { Role, ServicePrincipal, Policy, PolicyStatement } = require('aws-cdk-lib/aws-iam');
const { CfnDeliveryStream } = require('aws-cdk-lib/aws-kinesisfirehose');

class FirehoseStack extends Stack {
    /**
   *
   * @param {Construct} scope
   * @param {string} id
   * @param {StackProps=} props
   */
  constructor(scope, id, props) {
    super(scope, id, props);

    // setup the iam role for kinesis firehose
    this.kinesisFirehoseRole = new Role(this, "KinesisFirehoseRole", {
        assumedBy: new ServicePrincipal("firehose.amazonaws.com")
    });

    // setup the iam policy for kinesis firehose
    const firehosePolicy = new Policy(this, "KinesisFirehosePolicy", {
        statements: [
            new PolicyStatement({
                actions: [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject"
                ],
                resources: [`${props.bucket.bucketArn}`, `${props.bucket.bucketArn}/*`],
            }),
            new PolicyStatement({
                actions: [
                    "glue:GetTableVersions"
                ],
                resources: ["*"]
            })
        ]
    });

    // attach policy to role
    firehosePolicy.attachToRole(this.kinesisFirehoseRole);

    new CfnDeliveryStream(this, "FirehoseDbToS3", {
        deliveryStreamName: process.env.streamName,
        deliveryStreamType: "DirectPut",
        extendedS3DestinationConfiguration: {
            bucketArn: props.bucket.bucketArn,
            bufferingHints:{
                intervalInSeconds: parseInt(process.env.streamMaxIntervalInSec),
                sizeInMBs: parseInt(process.env.streamMaxFileSizeInMgb)
            },
            prefix: "lake/!{partitionKeyFromQuery:year}/!{partitionKeyFromQuery:month}/!{partitionKeyFromQuery:day}/!{partitionKeyFromQuery:hour}/",
            errorOutputPrefix: "error/",
            roleArn: this.kinesisFirehoseRole.roleArn,
            dataFormatConversionConfiguration:{
                enabled: true,
                inputFormatConfiguration: {
                    deserializer: {
                        openXJsonSerDe:{}
                    }
                },
                outputFormatConfiguration:{
                    serializer: {
                        parquetSerDe: {}
                    }
                },
                schemaConfiguration: {
                    roleArn: this.kinesisFirehoseRole.roleArn,
                    catalogId: Aws.ACCOUNT_ID,
                    databaseName: props.glueDb.databaseName,
                    tableName: props.glueTable.tableInput.name,
                    region: Aws.REGION,
                    versionId: "LATEST"
                }
            },
            dynamicPartitioningConfiguration: {
                enabled: true,
                retryOptions: {
                    durationInSeconds: 300
                }
            },
            processingConfiguration: {
                enabled: true,
                processors: [
                    {
                        type: "MetadataExtraction",
                        parameters: [
                            {
                                parameterName: "MetadataExtractionQuery",
                                parameterValue: '{year: .datehour| strftime("%Y"), month: .datehour| strftime("%m"), day: .datehour| strftime("%d"), hour: .datehour| strftime("%H")}'
                            },
                            {
                                parameterName: "JsonParsingEngine",
                                parameterValue: "JQ-1.6"
                            }
                        ]
                    },
                    {
                        type: "AppendDelimiterToRecord",
                        parameters: [
                            {
                                parameterName: "Delimiter",
                                parameterValue: "\\n"
                            }
                        ]
                    }
                ]
            }
        }
    })

  }
}

module.exports = { FirehoseStack }