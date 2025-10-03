// File: Services/LdapUserService.cs
// Target: .NET Framework + ASP.NET Web API (System.Web.Http), C# 7.3
using System;
using System.Collections.Generic;
using System.DirectoryServices;
using System.Linq;
using System.Text;

namespace Company.Directory
{
    public interface ILdapUserService
    {
        /// <summary>Get attributes for the currently logged-in Windows user (IIS Windows Auth).</summary>
        IDictionary<string, string> GetCurrentUserAttributes(IEnumerable<string> attributes);

        /// <summary>Get attributes by sAMAccountName (e.g., "jdoe").</summary>
        IDictionary<string, string> GetBySamAccountName(string samAccountName, IEnumerable<string> attributes);

        /// <summary>Get attributes by UPN (e.g., "jdoe@contoso.com").</summary>
        IDictionary<string, string> GetByUpn(string userPrincipalName, IEnumerable<string> attributes);
    }

    public sealed class LdapUserService : ILdapUserService
    {
        public IDictionary<string, string> GetCurrentUserAttributes(IEnumerable<string> attributes)
        {
            // Uses the worker process identity/Windows auth to bind; no password required.
            var sam = Environment.UserName; // "jdoe"
            return GetBySamAccountName(sam, attributes);
        }

        public IDictionary<string, string> GetBySamAccountName(string samAccountName, IEnumerable<string> attributes)
        {
            if (string.IsNullOrWhiteSpace(samAccountName))
                throw new ArgumentNullException(nameof(samAccountName));

            var filter = $"(&(objectCategory=person)(objectClass=user)(sAMAccountName={EscapeLdapFilter(samAccountName)}))";
            return QuerySingleUser(filter, attributes);
        }

        public IDictionary<string, string> GetByUpn(string userPrincipalName, IEnumerable<string> attributes)
        {
            if (string.IsNullOrWhiteSpace(userPrincipalName))
                throw new ArgumentNullException(nameof(userPrincipalName));

            var filter = $"(&(objectCategory=person)(objectClass=user)(userPrincipalName={EscapeLdapFilter(userPrincipalName)}))";
            return QuerySingleUser(filter, attributes);
        }

        // ----- Internals -----

        private static IDictionary<string, string> QuerySingleUser(string ldapFilter, IEnumerable<string> attributes)
        {
            var requested = NormalizeAttributes(attributes);

            var domainDn = GetDefaultNamingContext();
            if (string.IsNullOrWhiteSpace(domainDn))
                throw new InvalidOperationException("Could not read RootDSE.defaultNamingContext.");

            using (var root = new DirectoryEntry($"LDAP://{domainDn}")) // binds with app pool / Windows auth identity
            using (var searcher = new DirectorySearcher(root))
            {
                searcher.Filter = ldapFilter;
                searcher.PageSize = 1000;
                searcher.SizeLimit = 1;

                searcher.PropertiesToLoad.Clear();
                foreach (var a in requested) searcher.PropertiesToLoad.Add(a);
                if (!requested.Contains("distinguishedName", StringComparer.OrdinalIgnoreCase))
                    searcher.PropertiesToLoad.Add("distinguishedName");

                var res = searcher.FindOne();
                if (res == null)
                    return requested.ToDictionary(a => a, a => "(not found)", StringComparer.OrdinalIgnoreCase);

                return BuildResult(res, requested);
            }
        }

        private static IDictionary<string, string> BuildResult(SearchResult res, IEnumerable<string> requested)
        {
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var attr in requested)
                dict[attr] = ReadProp(res, attr);

            var dn = ReadProp(res, "distinguishedName");
            if (!string.IsNullOrWhiteSpace(dn))
                dict["distinguishedName"] = dn;

            return dict;
        }

        private static string ReadProp(SearchResult res, string prop)
        {
            if (!res.Properties.Contains(prop)) return "(not set)";
            var col = res.Properties[prop];
            if (col == null || col.Count == 0) return "(not set)";

            var items = new List<string>(col.Count);
            foreach (var v in col)
            {
                if (v == null) continue;
                if (v is byte[] bytes)
                    items.Add(BitConverter.ToString(bytes).Replace("-", "")); // hex
                else
                    items.Add(v.ToString());
            }
            return items.Count == 1 ? items[0] : string.Join("; ", items);
        }

        private static IReadOnlyList<string> NormalizeAttributes(IEnumerable<string> attributes)
        {
            var defaults = new[] { "displayName", "mail", "givenName", "sn", "userPrincipalName", "sAMAccountName" };
            if (attributes == null) return defaults;

            var list = new List<string>();
            foreach (var a in attributes)
            {
                if (string.IsNullOrWhiteSpace(a)) continue;
                // support comma-delimited strings passed in as a single value
                var parts = a.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                             .Select(s => s.Trim());
                list.AddRange(parts);
            }

            var distinct = list
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            return distinct.Count > 0 ? distinct : defaults;
        }

        private static string GetDefaultNamingContext()
        {
            try
            {
                using (var rootDse = new DirectoryEntry("LDAP://RootDSE"))
                    return rootDse.Properties["defaultNamingContext"]?.Value as string;
            }
            catch
            {
                return null;
            }
        }

        // RFC 4515 escaping for LDAP filter literals
        private static string EscapeLdapFilter(string input)
        {
            if (input == null) return null;
            var sb = new StringBuilder(input.Length);
            foreach (var c in input)
            {
                switch (c)
                {
                    case '\\': sb.Append(@"\5c"); break;
                    case '*':  sb.Append(@"\2a"); break;
                    case '(':  sb.Append(@"\28"); break;
                    case ')':  sb.Append(@"\29"); break;
                    case '\0': sb.Append(@"\00"); break;
                    default:   sb.Append(c); break;
                }
            }
            return sb.ToString();
        }
    }
}
