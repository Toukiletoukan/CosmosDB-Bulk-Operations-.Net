using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosDB.Playground
{
    public class FakeOrder
    {
        public string AccountNumber { get; set; }
        public string id { get; set; }
        public DateTime OrderDate { get; set; }
        public string Product { get; set; }
        public int DocumentIndex { get; set; }
        
    }
}
