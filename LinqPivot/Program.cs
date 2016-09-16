using NReco.PivotData;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace LinqPivot {
    class Program {

        static void Main(string[] args) {
            DataTable dt = LoadDataTable();
            Console.WriteLine("cols: {0}, rows: {1}", dt.Columns.Count, dt.Rows.Count);

            List<Customer> list = new List<Customer>();
            list.Add(new Customer() { Use = "Prev", Id = 1, Qty = 1 });
            list.Add(new Customer() { Use = "Prev", Id = 2, Qty = 5 });
            list.Add(new Customer() { Use = "Prev", Id = 3, Qty = 2 });
            list.Add(new Customer() { Use = "Prev", Id = 3, Qty = 1 });
            list.Add(new Customer() { Use = "Curr", Id = 1, Qty = 1 });
            list.Add(new Customer() { Use = "Curr", Id = 2, Qty = 2 });
            list.Add(new Customer() { Use = "Curr", Id = 2, Qty = 1 });
            list.Add(new Customer() { Use = "Curr", Id = 2, Qty = 1 });
            list.Add(new Customer() { Use = "Curr", Id = 3, Qty = 2 });
            list.Add(new Customer() { Use = "Next", Id = 1, Qty = 1 });
            list.Add(new Customer() { Use = "Next", Id = 1, Qty = 3 });
            list.Add(new Customer() { Use = "Next", Id = 3, Qty = 2 });

            Console.WriteLine("{0,4}, {1,4}, {2,4}", "Use", "Id", "Qty");
            foreach (Customer customer in list) {
                Console.WriteLine(String.Format("{0,4}, {1,4}, {2,4}", customer.Use, customer.Id, customer.Qty));
            }

            Console.WriteLine();
            Console.WriteLine("-------------- Method 0 -------------------");
            Console.WriteLine();

            PivotMethod_0(list);

            Console.WriteLine();
            Console.WriteLine("-------------- Method 1 -------------------");
            Console.WriteLine();

            PivotMethod_1(list);

            Console.WriteLine();
            Console.WriteLine("-------------- Method 2 -------------------");
            Console.WriteLine();

            PivotMethod_2(list);

            Console.WriteLine();
            Console.WriteLine("-------------- Method 3 -------------------");
            Console.WriteLine();

            PivotMethod_3(list);

            Console.WriteLine();
            Console.WriteLine("Press any key to stop...");
            Console.ReadKey();
        }

        public static DataTable LoadDataTable() {
            DataTable dt = new DataTable();
            using (var da = new SqlDataAdapter("SELECT * FROM IBIT_Analytics.dbo.broadband_reccomendations_na_product_key_23062016_ss", "data source=EVORA;initial catalog=IBIT_ANALYTICS;integrated security=True;MultipleActiveResultSets=True;App=EntityFramework")) {
                da.Fill(dt);
            }
            return dt;
        }

        private static void PivotMethod_1(List<Customer> list) {
            var pvtData = new PivotData(new[] { "Use", "Id" }, new SumAggregatorFactory("Qty"));

            // process the list
            pvtData.ProcessData(list, new ObjectMember().GetValue);

            Console.WriteLine("{0,2}, {1,4}, {2,4}, {3,4}", "Use", "  Id1", " Id2", "Id3");
            foreach (var x in pvtData) {
                //Console.WriteLine("{0,2}, {1,4}, {2,4}, {3,4}", x.Use, x.Id1, x.Id2, x.Id3);
            }
        }

        private static void PivotMethod_0(List<Customer> list) {
            var query = list
                .GroupBy(c => c.Use)
                .Select(g => new {
                    Use = g.Key,

                    Id1 = g.Where(c => c.Id == 1).Sum(c => c.Qty),
                    Id2 = g.Where(c => c.Id == 2).Sum(c => c.Qty),
                    Id3 = g.Where(c => c.Id == 3).Sum(c => c.Qty)
                });

            Console.WriteLine("{0,2}, {1,4}, {2,4}, {3,4}", "Use", "  Id1", " Id2", "Id3");
            foreach (var x in query) {
                Console.WriteLine("{0,2}, {1,4}, {2,4}, {3,4}", x.Use, x.Id1, x.Id2, x.Id3);
            }
        }

        private static void PivotMethod_2(List<Customer> list) {
            var ids = list.GroupBy(x => x.Id)
                .Select(g => new {
                    g.First().Id
                });

            var query = list
                .GroupBy(c => new { c.Use })
                .Select(g => {
                    dynamic x = new ExpandoObject();
                    var temp = x as IDictionary<string, Object>;
                    temp.Add("Use", g.Key.Use);

                    foreach (var id in ids) {
                        temp.Add("Id" + id.Id.ToString(), g.Where(c => c.Id == id.Id).Sum(c => c.Qty));
                    }
                    return x;
                });

            Console.WriteLine("{0,2}, {1,4}, {2,4}, {3,4}", "Use", "  Id1", " Id2", "Id3");
            foreach (var x in query) {
                Console.WriteLine("{0,2}, {1,4}, {2,4}, {3,4}", x.Use, x.Id1, x.Id2, x.Id3);
            }
        }

        private static void PivotMethod_3(List<Customer> list) {
            var query = list
                .GroupBy(c => new { c.Use, c.Id })
                .Select(g => new {
                    Group = g.Key,
                    Sum = g.Sum(c => c.Qty)
                });

            Console.WriteLine("{0,12}, {1,4}", "Group", " Sum");
            foreach (var x in query) {
                Console.WriteLine("{0,12}, {1,4}", x.Group, x.Sum);
            }

            var ids = list.GroupBy(x => x.Id)
                .Select(g => new {
                    g.First().Id
                }).ToList();

            var grps = query
                .GroupBy(g => g.Group.Use)
                .Select(g => new {
                    Use = g.Key,
                    Ids = g.Select(i => i.Group.Id).ToArray(),
                    Sums = g.Select(i => i.Sum).ToArray(),
                });

            Console.WriteLine();
            Console.WriteLine("{0,2}, {1,4}, {2,4}, {3,4}", "Use", "  Id1", " Id2", "Id3");
            foreach (var x in grps) {
                // more complex assignment of ids is required if ids are not in order from 1 to n incrementing by 1
                int[] idsarray = new int[ids.Count];
                for (int i = 0; i < x.Ids.Length; i++) {
                    var id = x.Ids[i];
                    var sum = x.Sums[i];
                    idsarray[id - 1] = sum;
                }
                Console.WriteLine("{0,2}, {1,4}, {2,4}, {3,4}", x.Use, idsarray[0], idsarray[1], idsarray[2]);
            }
        }
    }

    public class Customer {
        public string Use { get; set; }
        public int Id { get; set; }
        public int Qty { get; set; }
    }
}

