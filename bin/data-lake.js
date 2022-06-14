const cdk = require('aws-cdk-lib');
const dotenv = require("dotenv");
const { BucketStack } = require('../lib/BucketStack');
const { FirehoseStack } = require('../lib/FirehoseStack');
const { GlueStack } = require('../lib/GlueStack');

const envResult = dotenv.config({
  path: "./config.env"
});

const app = new cdk.App();

const bucketStack = new BucketStack(app, "BucketStack", {})

const glueStack = new GlueStack(app, "GlueStack", {
  bucket: bucketStack.bucket
})

const firehoseStack = new FirehoseStack(app, "FirehoseStack", {
  bucket: bucketStack.bucket,
  glueDb: glueStack.glueDb,
  glueTable: glueStack.glueTable
})