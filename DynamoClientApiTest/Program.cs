using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Amazon.DynamoDB;
using Amazon.DynamoDB.DataModel;
using Amazon.DynamoDB.DocumentModel;

namespace DynamoClientApiTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new AmazonDynamoDBClient();
            var context = new DynamoDBContext(client);

            context.Load<string>(null);

            context.Query<string>(null, QueryOperator.Between, null);

            //var condition = new ScanCondition("", ScanOp)
        }
    }
}
