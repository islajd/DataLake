const { Stack, Duration, RemovalPolicy } = require('aws-cdk-lib');
const { Bucket } = require('aws-cdk-lib/aws-s3');
// const sqs = require('aws-cdk-lib/aws-sqs');

class BucketStack extends Stack {
    /**
   *
   * @param {Construct} scope
   * @param {string} id
   * @param {StackProps=} props
   */
  constructor(scope, id, props) {
    super(scope, id, props);

    this.bucket = new Bucket(this, "DataLakeBucket", {
        publicReadAccess: false,
        removalPolicy: RemovalPolicy.DESTROY,
        bucketName: process.env.dataLakeName
    })
  }
}

module.exports = { BucketStack }