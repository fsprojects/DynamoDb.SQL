// Author : Yan Cui (twitter @theburningmonk)

// Email  : theburningmonk@gmail.com
// Blog   : http://theburningmonk.com

/*
    This script contains query and scan examples using the V2 API (with Index support).
    For more details on the query index, please check the Wiki page:
        https://github.com/theburningmonk/DynamoDb.SQL/wiki

    If you want to run these examples, please provide the AWS key and secret for your AWS 
    account and run the following functions from the 'Common'
    module first:
        createTable()   - creates a new table with 50 read and 50 write capacity
        seedData()      - seed the table with 5k items

    PLEASE DON'T FORGET TO DELETE THE TABLE AFTER RUNNING THE EXAMPLES. 
    I WILL NOT BE LIABLE FOR ANY AWS COSTS YOU INCUR WHILE RUNNING THESE EXAMPLES.
*/

using System;
using System.Diagnostics;
using System.Linq;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using DynamoDbV2.SQL.Execution;

namespace DynamoClientApiTest
{
    class Program
    {
        private const string AlienAdventure = "Alien Adventure";
        private const string AttackShips = "Attack Ships";
        private const string GalaxyInvaders = "Galaxy Invaders";
        private const string MeteorBlasters = "Meteor Blasters";
        private const string StarshipX = "Starship X";

        static void Main(string[] args)
        {
            var awsKey = "AKIAIJP66H5CFKU5DETQ";
            var awsSecret = "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";
            var client = new AmazonDynamoDBClient(awsKey, awsSecret, RegionEndpoint.USEast1);
            var ctx = new DynamoDBContext(client);

            var userId = "theburningmonk-1";

            // query examples
            QueryByHashKey(userId, client, ctx);
            QueryWithRangeKey(userId, client, ctx);
            QueryWithOrderAndLimit(userId, client, ctx);
            QueryWithNoConsistentRead(userId, client, ctx);
            ThrottlingWithQueryPageSize(userId, client, ctx);
            SelectSpecificAttributes(userId, client, ctx);
            QueryWithNoReturnedConsumedCapacity(userId, client);
            QueryWithLocalSecondaryIndexAllAttributes(userId, client, ctx);
            QueryWithLocalSecondaryIndexProjectedAttributes(userId, client, ctx);
            QueryWithGlobalSecondaryIndexProjectedAttributes(client, ctx);

            // scan examples
            BasicScan(client, ctx);
            ScanWithLimit(client, ctx);
            ThrottlingWithScanPageSize(client, ctx);
            ScanWithScanPageSizeAndSegments(client, ctx);
            ScanWithNoReturnedConsumedCapacity(client);

            Console.WriteLine("all done...");

            Console.ReadKey();
        }

        #region Query Examples

        private static void QueryByHashKey(string userId, AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE UserId = \"{0}\"", userId);

            Console.WriteLine("(AmazonDynamoDBClient) Running basic hash key query :\n\t\t{0}", selectQuery);
            var response = client.Query(selectQuery);
            Debug.Assert(response.Items.Count == 5);
            Debug.Assert(response.Items.TrueForAll(i => i["UserId"].S == userId));

            Console.WriteLine("(DynamoDBContext) Running basic hash key query :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecQuery<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Count() == 5);
            Debug.Assert(gameScores.All(gs => gs.UserId == userId));
        }

        private static void QueryWithRangeKey(string userId, AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE UserId = \"{0}\" AND GameTitle BEGINS WITH \"A\"", userId);

            Console.WriteLine("(AmazonDynamoDBClient) Running query with range key :\n\t\t{0}", selectQuery);
            var response = client.Query(selectQuery);
            Debug.Assert(response.Items.Count == 2);
            Debug.Assert(response.Items.TrueForAll(i => i["UserId"].S == userId));
            Debug.Assert(response.Items.TrueForAll(i => i["GameTitle"].S.StartsWith("A")));

            Console.WriteLine("(DynamoDBContext) Running query with range key :\n\t\t{0}", selectQuery);

            var gameScores = context.ExecQuery<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Count() == 2);
            Debug.Assert(gameScores.All(gs => gs.UserId == userId));
            Debug.Assert(gameScores.All(gs => gs.GameTitle.StartsWith("A")));
        }

        private static void QueryWithOrderAndLimit(string userId, AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE UserId = \"{0}\" ORDER ASC LIMIT 3", userId);

            Console.WriteLine("(AmazonDynamoDBClient) Running query with order and limit :\n\t\t{0}", selectQuery);
            var response = client.Query(selectQuery);
            Debug.Assert(response.Items.Count == 3);
            Debug.Assert(response.Items.TrueForAll(i => i["UserId"].S == userId));
            Debug.Assert(response.Items.TrueForAll(i => i["GameTitle"].S.CompareTo(MeteorBlasters) < 0));

            Console.WriteLine("(DynamoDBContext) Running query with order and limit :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecQuery<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Count() == 3);
            Debug.Assert(gameScores.All(gs => gs.UserId == userId));
            Debug.Assert(gameScores.All(gs => gs.GameTitle.CompareTo(MeteorBlasters) < 0));
        }

        private static void QueryWithNoConsistentRead(string userId, AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE UserId = \"{0}\" WITH (NoConsistentRead)", userId);

            Console.WriteLine("(AmazonDynamoDBClient) Running query with no consistent read :\n\t\t{0}", selectQuery);
            var response = client.Query(selectQuery);
            Debug.Assert(response.Items.Count == 5);
            Debug.Assert(response.Items.TrueForAll(i => i["UserId"].S == userId));

            Console.WriteLine("(DynamoDBContext) Running query with no consistent read :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecQuery<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Count() == 5);
            Debug.Assert(gameScores.All(gs => gs.UserId == userId));
        }

        private static void ThrottlingWithQueryPageSize(string userId, AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE UserId = \"{0}\" LIMIT 3 WITH (PageSize(1))", userId);

            Console.WriteLine("(AmazonDynamoDBClient) Running query with PageSize :\n\t\t{0}", selectQuery);
            var response = client.Query(selectQuery);
            Debug.Assert(response.Items.Count == 3);
            Debug.Assert(response.Items.TrueForAll(i => i["UserId"].S == userId));

            Console.WriteLine("(DynamoDBContext) Running query with PageSize :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecQuery<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Count() == 3);
            Debug.Assert(gameScores.All(gs => gs.UserId == userId));
        }

        private static void SelectSpecificAttributes(string userId, AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT UserId, GameTitle, Wins FROM GameScores WHERE UserId = \"{0}\"", userId);

            Console.WriteLine("(AmazonDynamoDBClient) Running query with specific attributes :\n\t\t{0}", selectQuery);
            var response = client.Query(selectQuery);
            Debug.Assert(response.Items.Count == 5);
            Debug.Assert(response.Items.TrueForAll(i => i["UserId"].S == userId));
            Debug.Assert(response.Items.TrueForAll(i => i.Count() == 3));

            Console.WriteLine("(DynamoDBContext) Running query with specific attributes :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecQuery<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Count() == 5);
            Debug.Assert(gameScores.All(gs => gs.UserId == userId));
            Debug.Assert(gameScores.All(gs => !string.IsNullOrWhiteSpace(gs.GameTitle) &&
                                              gs.Wins > 0 &&
                                              gs.Losses == 0 &&
                                              gs.TopScore == 0 &&
                                              gs.TopScoreDateTime == default(DateTime)));
        }

        private static void QueryWithNoReturnedConsumedCapacity(string userId, AmazonDynamoDBClient client)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE UserId = \"{0}\" WITH (NoReturnedCapacity)", userId);

            Console.WriteLine("(AmazonDynamoDBClient) Running query with no returned consumed capacity :\n\t\t{0}", selectQuery);
            var response = client.Query(selectQuery);
            Debug.Assert(response.Items.Count == 5);
            Debug.Assert(response.Items.TrueForAll(i => i["UserId"].S == userId));
            Debug.Assert(response.ConsumedCapacity == null);
        }

        private static void QueryWithLocalSecondaryIndexAllAttributes(string userId, AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE UserId = \"{0}\" AND TopScore >= 1000 WITH(Index(TopScoreIndex, true))", userId);

            Console.WriteLine("(AmazonDynamoDBClient) Running query with local secondary index (all attributes) :\n\t\t{0}", selectQuery);
            var response = client.Query(selectQuery);
            Debug.Assert(response.Items.Count > 0);
            Debug.Assert(response.Items.TrueForAll(i => i["UserId"].S == userId));
            Debug.Assert(response.Items.TrueForAll(i => int.Parse(i["TopScore"].N) >= 1000));
            Debug.Assert(response.Items.TrueForAll(i => i.Count == 6));

            Console.WriteLine("(DynamoDBContext) Running query with local secondary index (all attributes) :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecQuery<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Any());
            Debug.Assert(gameScores.All(gs => gs.UserId == userId));
            Debug.Assert(gameScores.All(gs => gs.TopScore >= 1000));
            Debug.Assert(gameScores.All(gs => gs.TopScoreDateTime != default(DateTime)));
        }

        private static void QueryWithLocalSecondaryIndexProjectedAttributes(string userId, AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE UserId = \"{0}\" AND TopScore >= 1000 WITH(Index(TopScoreIndex, false))", userId);

            Console.WriteLine("(AmazonDynamoDBClient) Running query with local secondary index (projected attributes) :\n\t\t{0}", selectQuery);
            var response = client.Query(selectQuery);
            Debug.Assert(response.Items.Count > 0);
            Debug.Assert(response.Items.TrueForAll(i => i["UserId"].S == userId));
            Debug.Assert(response.Items.TrueForAll(i => int.Parse(i["TopScore"].N) >= 1000));
            Debug.Assert(response.Items.TrueForAll(i => i.Count == 3 && !i.ContainsKey("TopScoreDateTime")));

            Console.WriteLine("(DynamoDBContext) Running query with local secondary index (projected attributes) :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecQuery<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Any());
            Debug.Assert(gameScores.All(gs => gs.UserId == userId));
            Debug.Assert(gameScores.All(gs => gs.TopScore >= 1000));
            Debug.Assert(gameScores.All(gs => gs.TopScoreDateTime == default(DateTime)));
        }

        private static void QueryWithGlobalSecondaryIndexProjectedAttributes(AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE GameTitle = \"{0}\" AND TopScore >= 1000 WITH(Index(GameTitleIndex, false), NoConsistentRead)", StarshipX);

            Console.WriteLine("(AmazonDynamoDBClient) Running query with global secondary index (projected attributes) :\n\t\t{0}", selectQuery);
            var response = client.Query(selectQuery);
            Debug.Assert(response.Items.Count > 0);
            Debug.Assert(response.Items.TrueForAll(i => !string.IsNullOrWhiteSpace(i["UserId"].S)));
            Debug.Assert(response.Items.TrueForAll(i => i["GameTitle"].S == StarshipX));
            Debug.Assert(response.Items.TrueForAll(i => int.Parse(i["TopScore"].N) >= 1000));
            Debug.Assert(response.Items.TrueForAll(i => i.Count == 3 && !i.ContainsKey("TopScoreDateTime")));

            Console.WriteLine("(DynamoDBContext) Running query with global secondary index (projected attributes) :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecQuery<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Any());
            Debug.Assert(gameScores.All(gs => !string.IsNullOrWhiteSpace(gs.UserId)));
            Debug.Assert(gameScores.All(gs => gs.GameTitle == StarshipX));
            Debug.Assert(gameScores.All(gs => gs.TopScore >= 1000));
            Debug.Assert(gameScores.All(gs => gs.TopScoreDateTime == default(DateTime)));
        }

        #endregion

        #region Scan Examples

        private static void BasicScan(AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE GameTitle = \"{0}\"", StarshipX);

            Console.WriteLine("(AmazonDynamoDBClient) Running basic scan :\n\t\t{0}", selectQuery);
            var response = client.Scan(selectQuery);
            Debug.Assert(response.Items.Count == 1000);
            Debug.Assert(response.Items.TrueForAll(i => i["GameTitle"].S == StarshipX));

            Console.WriteLine("(DynamoDBContext) Running basic scan :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecScan<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Count() == 1000);
            Debug.Assert(gameScores.All(gs => gs.GameTitle == StarshipX));
        }

        private static void ScanWithLimit(AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE GameTitle = \"{0}\" LIMIT 10", StarshipX);

            Console.WriteLine("(AmazonDynamoDBClient) Running scan with limit :\n\t\t{0}", selectQuery);
            var response = client.Scan(selectQuery);
            Debug.Assert(response.Items.Count == 10);
            Debug.Assert(response.Items.TrueForAll(i => i["GameTitle"].S == StarshipX));

            Console.WriteLine("(DynamoDBContext) Running scan with limit :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecScan<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Count() == 10);
            Debug.Assert(gameScores.All(gs => gs.GameTitle == StarshipX));
        }

        private static void ThrottlingWithScanPageSize(AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE GameTitle = \"{0}\" WITH (PageSize(20))", StarshipX);

            Console.WriteLine("(AmazonDynamoDBClient) Running scan with PageSize :\n\t\t{0}", selectQuery);
            var response = client.Scan(selectQuery);
            Debug.Assert(response.Items.Count == 1000);
            Debug.Assert(response.Items.TrueForAll(i => i["GameTitle"].S == StarshipX));

            Console.WriteLine("(DynamoDBContext) Running scan with PageSize :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecScan<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Count() == 1000);
            Debug.Assert(gameScores.All(gs => gs.GameTitle == StarshipX));
        }

        private static void ScanWithScanPageSizeAndSegments(AmazonDynamoDBClient client, DynamoDBContext context)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE GameTitle = \"{0}\" WITH (PageSize(20), Segments(2))", StarshipX);

            Console.WriteLine("(AmazonDynamoDBClient) Running scan with PageSize and 2 segments :\n\t\t{0}", selectQuery);
            var response = client.Scan(selectQuery);
            Debug.Assert(response.Items.Count == 1000);
            Debug.Assert(response.Items.TrueForAll(i => i["GameTitle"].S == StarshipX));

            Console.WriteLine("(DynamoDBContext) Running scan with PageSize and 2 segments :\n\t\t{0}", selectQuery);
            var gameScores = context.ExecScan<GameScore>(selectQuery).ToArray();
            Debug.Assert(gameScores.Count() == 1000);
            Debug.Assert(gameScores.All(gs => gs.GameTitle == StarshipX));
        }

        private static void ScanWithNoReturnedConsumedCapacity(AmazonDynamoDBClient client)
        {
            var selectQuery = string.Format("SELECT * FROM GameScores WHERE GameTitle = \"{0}\" WITH (NoReturnedCapacity)", StarshipX);

            Console.WriteLine("(AmazonDynamoDBClient) Running scan with NoReturnedCapacity :\n\t\t{0}", selectQuery);
            var response = client.Scan(selectQuery);
            Debug.Assert(response.Items.Count == 1000);
            Debug.Assert(response.Items.TrueForAll(i => i["GameTitle"].S == StarshipX));
            Debug.Assert(response.ConsumedCapacity == null);
        }

        #endregion

        [DynamoDBTable("GameScores")]
        public class GameScore
        {
            [DynamoDBHashKey]
            public string UserId { get; set; }

            [DynamoDBRangeKey]
            public string GameTitle { get; set; }
            
            public int TopScore { get; set; }

            public DateTime TopScoreDateTime { get; set; }

            public int Wins { get; set; }

            public int Losses { get; set; }
        }        
    }
}
