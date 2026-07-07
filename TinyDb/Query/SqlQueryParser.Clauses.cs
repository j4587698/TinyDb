using System.Globalization;

namespace TinyDb.Query;

internal sealed partial class SqlQueryParser
{

    private List<SqlOrderBy> ParseOrderBy(string orderText)
    {
        if (string.IsNullOrWhiteSpace(orderText))
        {
            throw Error("ORDER BY clause cannot be empty.");
        }

        var result = new List<SqlOrderBy>();
        foreach (var item in SplitTopLevelComma(orderText))
        {
            var itemParser = new OrderItemParser(item);
            result.Add(itemParser.Parse());
        }

        return result;
    }

    private List<SqlProjectionField> ParseProjection(string projectionText)
    {
        if (string.IsNullOrWhiteSpace(projectionText))
        {
            throw Error("SELECT list cannot be empty.");
        }

        if (projectionText == "*")
        {
            return new List<SqlProjectionField>();
        }

        var result = new List<SqlProjectionField>();
        foreach (var item in SplitTopLevelComma(projectionText))
        {
            if (string.IsNullOrWhiteSpace(item))
            {
                throw Error("SELECT field cannot be empty.");
            }

            var itemParser = new ProjectionItemParser(item);
            result.Add(itemParser.Parse());
        }

        return result;
    }

    private List<SqlAssignment> ParseAssignments(string assignmentsText)
    {
        var result = new List<SqlAssignment>();
        foreach (var item in SplitTopLevelComma(assignmentsText))
        {
            if (string.IsNullOrWhiteSpace(item))
            {
                throw Error("SET assignment cannot be empty.");
            }

            var itemParser = new AssignmentParser(item, _parameters);
            result.Add(itemParser.Parse());
        }

        ValidateNoDuplicateFields(result.Select(static assignment => assignment.FieldPath), "SET clause");
        return result;
    }

}
