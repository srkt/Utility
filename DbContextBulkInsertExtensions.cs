public static class DbContextBulkInsertExtensions
{
    public static async Task BulkInsertAsync<T>(this DbContext context, List<T> entities) where T : class
    {
        if (entities == null || entities.Count == 0)
            return;

        string tableName = GetOracleTableName<T>(context);

        await Task.Run(() =>
        {
            var dataTable = ToDataTable(entities, context);

            using (var conn = new OracleConnection(context.Database.Connection.ConnectionString))
            {
                conn.Open();

                using (var bulkCopy = new OracleBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = tableName;

                    foreach (DataColumn column in dataTable.Columns)
                        bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);

                    bulkCopy.WriteToServer(dataTable);
                }
            }
        });
    }

    private static string GetOracleTableName<T>(DbContext context) where T : class
    {
        var objectContext = ((IObjectContextAdapter)context).ObjectContext;
        var metadata = objectContext.MetadataWorkspace;

        var entityType = typeof(T);
        var entitySet = metadata
            .GetItems<EntityContainer>(DataSpace.SSpace)
            .SelectMany(c => c.BaseEntitySets)
            .FirstOrDefault(s => s.ElementType.Name == entityType.Name || s.Name == entityType.Name);

        if (entitySet == null)
            throw new Exception($"Unable to find table mapping for entity {entityType.Name}");

        return entitySet.MetadataProperties
            .Where(p => p.Name.Equals("Table", StringComparison.OrdinalIgnoreCase))
            .Select(p => p.Value?.ToString())
            .FirstOrDefault() ?? entitySet.Name;
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
