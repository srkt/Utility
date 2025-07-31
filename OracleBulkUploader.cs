using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Data.Metadata.Edm;
using System.Data.Objects;
using System.Linq;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

public static class OracleBulkUploader
{
    public static async Task BulkInsertAsync<T>(List<T> entities, string tableName, DbContext context) where T : class
    {
        var dataTable = ToDataTable(entities, context);

        var connectionString = context.Database.Connection.ConnectionString;
        using (var conn = new OracleConnection(connectionString))
        {
            await conn.OpenAsync();

            using (var bulkCopy = new OracleBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = tableName;

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dataTable);
            }
        }
    }

    private static DataTable ToDataTable<T>(List<T> entities, DbContext context) where T : class
    {
        var table = new DataTable();
        var entityType = typeof(T);

        var objectContext = ((IObjectContextAdapter)context).ObjectContext;
        var metadata = objectContext.MetadataWorkspace;

        var entitySet = metadata
            .GetItems<EntityContainer>(DataSpace.CSpace)
            .SelectMany(c => c.EntitySets)
            .FirstOrDefault(s => s.ElementType.Name == entityType.Name);

        if (entitySet == null)
            throw new InvalidOperationException($"EntitySet not found for type {entityType.Name}");

        var props = entitySet.ElementType.Properties;

        foreach (var prop in props)
        {
            var propInfo = entityType.GetProperty(prop.Name);
            if (propInfo == null) continue;

            var type = Nullable.GetUnderlyingType(propInfo.PropertyType) ?? propInfo.PropertyType;
            table.Columns.Add(prop.Name, type);
        }

        foreach (var entity in entities)
        {
            var row = table.NewRow();
            foreach (var prop in props)
            {
                var propInfo = entityType.GetProperty(prop.Name);
                if (propInfo != null)
                    row[prop.Name] = propInfo.GetValue(entity) ?? DBNull.Value;
            }
            table.Rows.Add(row);
        }

        return table;
    }
}
