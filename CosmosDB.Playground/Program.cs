using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkUpdate;
using System.Globalization;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;

namespace CosmosDB.Playground
{
    class Program
    {
        private static string _documentdbUrl = "https://XXXXX.documents.azure.com:443/";
        private static string _documentdbKey = "";
        private static string _database = "cosmosdbplayground";
        private static string _collection = "orders";

        private static DocumentClient _documentClient;
        private static DocumentCollection _documentCollection;

        private const string BASE_ACCOUNT_PREFIX = "AC-ID";

        private static int _numberOfDocumentsToGenerate = 1000;
        private static int _numberOfPartitions = 50;

        static void Main(string[] args)
        {
            AskForInputs();

            InitializeDatabaseAndCollection(_database, _collection).GetAwaiter().GetResult();

            var initialDocuments = GenerateFakeOrders(_numberOfDocumentsToGenerate, _numberOfPartitions, BASE_ACCOUNT_PREFIX);

            Console.WriteLine($"Starting bulk insert of {_numberOfPartitions * _numberOfDocumentsToGenerate} documents ...");

            var importResult = BulkInsertDocuments(_database, _collection, initialDocuments, upsert:true).GetAwaiter().GetResult();

            Console.WriteLine($"Import of {importResult.NumberOfDocumentsImported} documents in {importResult.TotalTimeTaken}");

            List<UpdateItem> updateList = initialDocuments.Select(d =>
                new UpdateItem(
                    d.id,
                    d.AccountNumber,
                    new List<UpdateOperation> {
                        new SetUpdateOperation<string>(
                            "NewSimpleProperty", 
                            "New Property Value"),
                        new SetUpdateOperation<dynamic>(
                            "NewComplexProperty", 
                            new {
                                prop1 = "Hello",
                                prop2 = "World!"
                            }),
                        new UnsetUpdateOperation(nameof(FakeOrder.DocumentIndex)),
                    }))
                .ToList();

            var updateSetResult = BulkUpdatetDocuments(_database, _collection, updateList).GetAwaiter().GetResult();

            Console.WriteLine($"Update of {updateSetResult.NumberOfDocumentsUpdated} documents in {updateSetResult.TotalTimeTaken}");

            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        public static void AskForInputs()
        {
            Console.WriteLine("How many documents to insert by partition ?");
            bool parsedDocuments = Int32.TryParse(Console.ReadLine(), out int parsedDocumentsResult);

            _numberOfDocumentsToGenerate = parsedDocuments ? parsedDocumentsResult : _numberOfDocumentsToGenerate;

            Console.WriteLine("How many partitions to generate ?");
            bool parsedPartitions = Int32.TryParse(Console.ReadLine(), out int parsedPartitionsResult);

            _numberOfPartitions = parsedPartitions ? parsedPartitionsResult : _numberOfPartitions;
        }

        public static async Task InitializeDatabaseAndCollection(
            string databaseName,
            string collectionName,
            int throughput = 1000)
        {
            Uri serviceEndpoint = new Uri(_documentdbUrl);

            _documentClient = new DocumentClient(serviceEndpoint, _documentdbKey);

            _documentCollection = new DocumentCollection();
            _documentCollection.Id = _collection;
            _documentCollection.PartitionKey.Paths.Add("/AccountNumber");
            _documentCollection.IndexingPolicy.Automatic = true;
            _documentCollection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;

            await DropCollectionIfExistAsync(databaseName, collectionName);

            // Create database
            Database database = new Database() { Id = _database };

            await _documentClient.CreateDatabaseIfNotExistsAsync(database);

            // Create collection
            await _documentClient.CreateDocumentCollectionIfNotExistsAsync(
                        UriFactory.CreateDatabaseUri(_database),
                        _documentCollection,
                        new RequestOptions { OfferThroughput = throughput });
        }

        public static async Task DropCollectionIfExistAsync(
            string database,
            string collection)
        {
            var existingCollection = await _documentClient.ReadDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(database, collection));
            if (existingCollection != null)
            {
                await _documentClient.DeleteDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(database, collection));
            }
        }

        public static IEnumerable<FakeOrder> GenerateFakeOrders(
            int numberToGeneratePerPartition,
            int numberOfPartitions,
            string accountIdPrefix = "Account")
        {
            List<FakeOrder> result = new List<FakeOrder>();

            for (int x = 0; x < numberOfPartitions; x++)
            {
                for (int i = 0; i < numberToGeneratePerPartition; i++)
                {
                    result.Add(new FakeOrder
                    {
                        AccountNumber = $"{accountIdPrefix}-{x}",
                        OrderDate = DateTime.UtcNow.AddDays(-i),
                        DocumentIndex = i,
                        Product = $"Produit XXX-{i}",
                        id = $"{(accountIdPrefix + x).ToString()}-{x}-{i}"
                    });
                }
            }

            return result;
        }

        public static async Task<BulkImportResponse> BulkInsertDocuments(
            string database,
            string collection,
            IEnumerable<object> documents,
            bool upsert = false)
        {
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(database, collection);
            var collectionResource = await _documentClient.ReadDocumentCollectionAsync(collectionUri);
            var executor = new BulkExecutor(_documentClient, collectionResource);

            await executor.InitializeAsync();

            return await executor.BulkImportAsync(documents, enableUpsert: upsert);
        }

        public static async Task<BulkUpdateResponse> BulkUpdatetDocuments(
            string database,
            string collection,
            IEnumerable<UpdateItem> updates)
        {
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(database, collection);
            var collectionResource = await _documentClient.ReadDocumentCollectionAsync(collectionUri);
            var executor = new BulkExecutor(_documentClient, collectionResource);

            await executor.InitializeAsync();

            return await executor.BulkUpdateAsync(updates);
        }
    }
}
