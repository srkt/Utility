using System;
using System.Collections.Generic;
using System.DirectoryServices;
using System.Text;
using System.Linq;
using System.DirectoryServices.Protocols; // keep so your signatures keep working

namespace Pmis.Ldap
{
    public enum FilterComp { None, And, Or, Not }

    public sealed class LdapEntryItem : Dictionary<string, string> { }

    public static class LdapUtility
    {
        // ---------- Your existing helper signatures ----------
        public static Func<string, string> GenerateLDAPQueryString(string attributes, FilterComp condition = FilterComp.None)
        {
            if (string.IsNullOrWhiteSpace(attributes))
                throw new ArgumentNullException(nameof(attributes), "attributes cannot be null");

            var op = GetFilterCompString(condition);
            var parts = attributes.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                                  .Select(a => a.Trim())
                                  .ToArray();

            if (condition == FilterComp.Not && parts.Length > 1)
                throw new ArgumentException("NOT with multiple attributes is ambiguous in simple builder.");

            if (condition == FilterComp.Not)
                return (value) => string.IsNullOrEmpty(value)
                    ? throw new ArgumentNullException(nameof(value), "value cannot be null")
                    : $"(!({parts[0]}={Escape(value)}))";

            // default to AND when None (keeps old behavior that grouped)
            if (op.Length == 0) op = "&";

            return (value) =>
            {
                if (string.IsNullOrEmpty(value))
                    throw new ArgumentNullException(nameof(value), "value cannot be null");

                var sb = new StringBuilder();
                sb.Append('(').Append(op);
                foreach (var p in parts)
                    sb.Append('(').Append(p).Append('=').Append(Escape(value)).Append(')');
                sb.Append(')');
                return sb.ToString();
            };
        }

        public static string GetFilterCompString(FilterComp condition)
        {
            switch (condition)
            {
                case FilterComp.And: return "&";
                case FilterComp.Or:  return "|";
                case FilterComp.Not: return "!";
                default:             return string.Empty;
            }
        }

        // ---------- LOOKUP: same signatures as before ----------

        // Overload with LdapConnection parameter kept for compatibility.
        public static List<LdapEntryItem> LookUp(LdapConnection ldapConnection,
                                                 LdapOptions options,
                                                 string searchString,
                                                 string[] searchAttributes)
        {
            // We intentionally ignore the socket and do the search through S.DS.
            return LookupWithDirectoryServices(options, searchString, searchAttributes);
        }

        public static List<LdapEntryItem> LookUp(LdapOptions options,
                                                 string searchString,
                                                 string[] searchAttributes)
        {
            return LookupWithDirectoryServices(options, searchString, searchAttributes);
        }

        // ---------- VALIDATE USER: same signature ----------

        public static bool ValidateUser(LdapOptions options, string userName, string password)
        {
            // Try resolve DN first (using UserSearchKey if provided)
            string userDn = TryResolveUserDn(options, userName);

            // Choose best identity to bind with
            string bindIdentity = !string.IsNullOrEmpty(userDn)
                                  ? userDn
                                  : BuildDomainQualified(options, userName);

            try
            {
                using (var entry = new DirectoryEntry(BuildLdapPath(options),
                                                      bindIdentity,
                                                      password,
                                                      MapAuthTypes(options)))
                {
                    // Force bind
                    var _ = entry.NativeObject;
                    return true;
                }
            }
            catch
            {
                return false;
            }
        }

        // ---------- (Optional) Keep this if other code calls it ----------
        public static LdapConnection GetLdapConnection(LdapOptions options)
        {
            // If other code still expects a Protocols connection, keep returning it.
            var id = new LdapDirectoryIdentifier(options.HostName, options.PortNumber, true, false);
            var conn = new LdapConnection(id)
            {
                AuthType = options.AuthenticationType
            };
            if (!string.IsNullOrEmpty(options.UserName))
                conn.Credential = new System.Net.NetworkCredential(options.UserName, options.Password);

            if (options.EnableSsl)
                conn.SessionOptions.SecureSocketLayer = true;

            return conn;
        }

        // =====================================================
        // =============== Internal S.DS helpers ===============
        // =====================================================

        private static List<LdapEntryItem> LookupWithDirectoryServices(LdapOptions options,
                                                                       string filter,
                                                                       string[] attrs)
        {
            if (string.IsNullOrWhiteSpace(filter))
                throw new ArgumentNullException(nameof(filter));

            var list = new List<LdapEntryItem>();

            using (var root = new DirectoryEntry(BuildLdapPath(options),
                                                 options.UserName,
                                                 options.Password,
                                                 MapAuthTypes(options)))
            {
                using (var ds = new DirectorySearcher(root))
                {
                    ds.Filter = filter;
                    ds.SearchScope = MapScope(options.Scope);
                    ds.PageSize = 1000;

                    if (attrs != null && attrs.Length > 0)
                    {
                        ds.PropertiesToLoad.Clear();
                        foreach (var a in attrs) ds.PropertiesToLoad.Add(a);
                    }

                    foreach (SearchResult sr in ds.FindAll())
                    {
                        var item = new LdapEntryItem();

                        if (attrs != null && attrs.Length > 0)
                        {
                            foreach (var a in attrs)
                            {
                                var values = sr.Properties[a];
                                item[a] = (values != null && values.Count > 0) ? (values[0] != null ? values[0].ToString() : "") : "";
                            }
                        }
                        else
                        {
                            foreach (string propName in sr.Properties.PropertyNames)
                            {
                                var values = sr.Properties[propName];
                                item[propName] = (values != null && values.Count > 0) ? (values[0] != null ? values[0].ToString() : "") : "";
                            }
                        }

                        list.Add(item);
                    }
                }
            }

            return list;
        }

        private static string TryResolveUserDn(LdapOptions options, string userName)
        {
            if (string.IsNullOrEmpty(userName)) return null;

            // If a DN already
            if (userName.IndexOf("DC=", StringComparison.OrdinalIgnoreCase) >= 0 ||
                userName.IndexOf("CN=", StringComparison.OrdinalIgnoreCase) >= 0)
                return userName;

            using (var root = new DirectoryEntry(BuildLdapPath(options),
                                                 options.UserName,
                                                 options.Password,
                                                 MapAuthTypes(options)))
            {
                using (var ds = new DirectorySearcher(root))
                {
                    ds.SearchScope = MapScope(options.Scope);
                    ds.PageSize = 1;
                    ds.PropertiesToLoad.Add("distinguishedName");

                    if (!string.IsNullOrWhiteSpace(options.UserSearchKey))
                        ds.Filter = "(" + Escape(options.UserSearchKey) + "=" + Escape(userName) + ")";
                    else
                        ds.Filter = "(|(sAMAccountName=" + Escape(userName) + ")(userPrincipalName=" + Escape(userName) + "))";

                    var result = ds.FindOne();
                    if (result != null && result.Properties.Contains("distinguishedName"))
                        return result.Properties["distinguishedName"][0] != null
                            ? result.Properties["distinguishedName"][0].ToString()
                            : null;
                }
            }

            return null;
        }

        private static string BuildLdapPath(LdapOptions options)
        {
            var scheme = options.EnableSsl ? "LDAPS" : "LDAP";
            // LDAP://host:port/BASE_DN
            var baseDn = string.IsNullOrWhiteSpace(options.SearchStartAt) ? "" : options.SearchStartAt;
            return scheme + "://" + options.HostName + ":" + options.PortNumber + "/" + baseDn;
        }

        private static AuthenticationTypes MapAuthTypes(LdapOptions options)
        {
            // Map Protocols.AuthType to DirectoryServices AuthenticationTypes
            AuthenticationTypes at;
            switch (options.AuthenticationType)
            {
                case AuthType.Anonymous:
                    at = AuthenticationTypes.Anonymous;
                    break;
                case AuthType.Basic:
                    at = AuthenticationTypes.None;   // basic/clear; prefer Secure where possible
                    break;
                case AuthType.Negotiate:
                    at = AuthenticationTypes.Secure;
                    break;
                default:
                    at = AuthenticationTypes.Secure;
                    break;
            }

            if (options.EnableSsl) at |= AuthenticationTypes.SecureSocketsLayer;
            return at;
        }

        private static SearchScope MapScope(System.DirectoryServices.Protocols.SearchScope scopeP)
        {
            switch (scopeP)
            {
                case System.DirectoryServices.Protocols.SearchScope.Base:
                    return SearchScope.Base;
                case System.DirectoryServices.Protocols.SearchScope.OneLevel:
                    return SearchScope.OneLevel;
                default:
                    return SearchScope.Subtree;
            }
        }

        private static string BuildDomainQualified(LdapOptions options, string userName)
        {
            if (string.IsNullOrWhiteSpace(options.Domain)) return userName;
            if (userName.Contains("@")) return userName; // looks like UPN already
            return options.Domain + "\\" + userName;
        }

        // RFC4515-ish escaping for filter values
        private static string Escape(string value)
        {
            if (value == null) return null;
            return value
                .Replace(@"\", @"\5c")
                .Replace("*", @"\2a")
                .Replace("(", @"\28")
                .Replace(")", @"\29")
                .Replace("\0", @"\00");
        }
    }
}
