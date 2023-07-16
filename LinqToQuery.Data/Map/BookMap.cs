using Dapper.FluentMap.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToQuery.Infra.Map
{
    public class MyObjectMap : EntityMap<MyEntity>
    {
        public MyObjectMap()
        {
            Map(p => p.Id).ToColumn("id_table");
            Map(p => p.MyProp).ToColumn("prop_table");
            Map(p => p.OtherProp).ToColumn("other_prop_table");
        }
    }
}
