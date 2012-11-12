using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Amazon.DynamoDB;
using Amazon.DynamoDB.DataModel;
using Amazon.DynamoDB.DocumentModel;

using DynamoDb.SQL.Execution;

namespace DynamoClientApiTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new AmazonDynamoDBClient();
            var cxt = new DynamoDBContext(client);

            // put the test data there to run the test with
            //PutTestData(cxt, GetTestData());

            // query all
            //var hashKey = "\"Amazon DynamoDB#DynamoDB Thread 1\"";
            //var queryAllRes = client.QueryAsyncAsTask("SELECT * from Reply where @HashKey = " + hashKey).Result;

            // query subset
            //var querySubsetRes1 = client.QueryAsyncAsTask("select * from Reply where @HashKey = " + hashKey + " and @RangeKey >= \"2012-06-10\"").Result;
            //var querySubsetRes2 = client.QueryAsyncAsTask("select * from Reply where @HashKey = " + hashKey + " and @RangeKey < \"2012-06-10\"").Result;
            
            //var querySubsetRes3 = cxt.ExecQuery<Reply>("select * from Reply where @HashKey = " + hashKey + " and @RangeKey >= \"2012-06-10\"").ToArray();
            //var querySubsetRes4 = cxt.ExecQuery<Reply>("select * from Reply where @HashKey = " + hashKey + " and @RangeKey < \"2012-06-10\"").ToArray();

            //var scanRes1 = client.ScanAsyncAsTask("Select * from Reply where PostedBy contains \"A\"").Result;
            //var scanRes2 = client.ScanAsyncAsTask("Select * from Reply where ReplyDateTime between \"2012-06-10\" and \"2012-06-20\"").Result;

            //var scanRes3 = cxt.ExecScan<Reply>("Select * from Reply where PostedBy contains \"A\"");
            //var scanRes4 = cxt.ExecScan<Reply>("Select * from Reply where ReplyDateTime between \"2012-06-10\" and \"2012-06-20\"");
            
            Console.ReadKey();
        }

        private static Reply[] GetTestData()
        {
            return new[]
            {
                new Reply
                {
                    Id = "Amazon DynamoDB#DynamoDB Thread 1",
                    ReplyDateTime = DateTime.Parse("2012-06-03T13:29:39.054Z"),
                    Message = "DynamoDB Thread 1 Reply 1 text",
                    PostedBy = "User A"
                },
                new Reply
                {
                    Id = "Amazon DynamoDB#DynamoDB Thread 1",
                    ReplyDateTime = DateTime.Parse("2012-06-10T13:29:39.317Z"),
                    Message = "DynamoDB Thread 1 Reply 2 text",
                    PostedBy = "User B"
                },
                new Reply
                {
                    Id = "Amazon DynamoDB#DynamoDB Thread 1",
                    ReplyDateTime = DateTime.Parse("2012-06-17T13:29:39.578Z"),
                    Message = "DynamoDB Thread 1 Reply 3 text",
                    PostedBy = "User B"
                },
                new Reply
                {
                    Id = "Amazon DynamoDB#DynamoDB Thread 2",
                    ReplyDateTime = DateTime.Parse("2012-06-17T13:29:39.843Z"),
                    Message = "DynamoDB Thread 2 Reply 1 text",
                    PostedBy = "User A"
                },
                new Reply
                {
                    Id = "Amazon DynamoDB#DynamoDB Thread 2",
                    ReplyDateTime = DateTime.Parse("2012-06-23T13:29:40.114Z"),
                    Message = "DynamoDB Thread 2 Reply 2 text",
                    PostedBy = "User A"
                }
            };
        }

        private static void PutTestData(DynamoDBContext cxt, Reply[] replies)
        {
            var batchWrite = cxt.CreateBatchWrite<Reply>();
            batchWrite.AddPutItems(replies);
            
            cxt.ExecuteBatchWrite(batchWrite);
        }

        [DynamoDBTable("Reply")]
        public class Reply
        {
            [DynamoDBHashKey]
            public string Id { get; set; }

            [DynamoDBRangeKey]
            public DateTime ReplyDateTime { get; set; }
            
            public string Message { get; set; }

            public string PostedBy { get; set; }
        }        
    }
}
