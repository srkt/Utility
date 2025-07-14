using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Web.Mvc;
using Oracle.ManagedDataAccess.Client;

public class FacilityDto
{
    public int FacilityId { get; set; }
    public string FacilityName { get; set; }
    public string Address { get; set; }
    public string City { get; set; }
    public string State { get; set; }
    public string Zip { get; set; }
    public string Phone { get; set; }
    public string Fax { get; set; }
    public string ContactTitle { get; set; }
    public string ContactPerson { get; set; }
    public string Note { get; set; }
    public DateTime? LastUpdated { get; set; }
    public DateTime? LastInserted { get; set; }
    public string IsDeleted { get; set; }
}

public class FacilityManager
{
    private readonly string connectionString = "User Id=your_user;Password=your_password;Data Source=your_datasource;";

    public async Task CreateFacilityAsync(FacilityDto facility)
    {
        using (var conn = new OracleConnection(connectionString))
        {
            await conn.OpenAsync();
            string sql = @"
                INSERT INTO WMGMT_RCV_FACILITY (
                    RCV_FACILITY_ID, RCV_FACILITY_NAME, ADDRESS, CITY, RCV_FACILITY_STATE, ZIP,
                    PHONE, FAX, CONTACT_TITLE, CONTACT_PERSON, NOTE, LAST_INSERTED, IS_DELETED
                ) VALUES (
                    WMGMT_RCV_SEQ.NEXTVAL, :name, :address, :city, :state, :zip,
                    :phone, :fax, :contactTitle, :contactPerson, :note, SYSDATE, 'N'
                )";

            using (var cmd = new OracleCommand(sql, conn))
            {
                cmd.Parameters.Add(":name", facility.FacilityName);
                cmd.Parameters.Add(":address", facility.Address);
                cmd.Parameters.Add(":city", facility.City);
                cmd.Parameters.Add(":state", facility.State);
                cmd.Parameters.Add(":zip", facility.Zip);
                cmd.Parameters.Add(":phone", string.IsNullOrEmpty(facility.Phone) ? DBNull.Value : (object)facility.Phone);
                cmd.Parameters.Add(":fax", string.IsNullOrEmpty(facility.Fax) ? DBNull.Value : (object)facility.Fax);
                cmd.Parameters.Add(":contactTitle", string.IsNullOrEmpty(facility.ContactTitle) ? DBNull.Value : (object)facility.ContactTitle);
                cmd.Parameters.Add(":contactPerson", string.IsNullOrEmpty(facility.ContactPerson) ? DBNull.Value : (object)facility.ContactPerson);
                cmd.Parameters.Add(":note", string.IsNullOrEmpty(facility.Note) ? DBNull.Value : (object)facility.Note);
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    public async Task<FacilityDto> ReadFacilityAsync(int id)
    {
        using (var conn = new OracleConnection(connectionString))
        {
            await conn.OpenAsync();
            string sql = "SELECT * FROM WMGMT_RCV_FACILITY WHERE RCV_FACILITY_ID = :id AND IS_DELETED = 'N'";

            using (var cmd = new OracleCommand(sql, conn))
            {
                cmd.Parameters.Add(":id", id);
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        return new FacilityDto
                        {
                            FacilityId = Convert.ToInt32(reader["RCV_FACILITY_ID"]),
                            FacilityName = reader["RCV_FACILITY_NAME"]?.ToString(),
                            Address = reader["ADDRESS"]?.ToString(),
                            City = reader["CITY"]?.ToString(),
                            State = reader["RCV_FACILITY_STATE"]?.ToString(),
                            Zip = reader["ZIP"]?.ToString(),
                            Phone = reader["PHONE"] == DBNull.Value ? null : reader["PHONE"].ToString(),
                            Fax = reader["FAX"] == DBNull.Value ? null : reader["FAX"].ToString(),
                            ContactTitle = reader["CONTACT_TITLE"] == DBNull.Value ? null : reader["CONTACT_TITLE"].ToString(),
                            ContactPerson = reader["CONTACT_PERSON"] == DBNull.Value ? null : reader["CONTACT_PERSON"].ToString(),
                            Note = reader["NOTE"] == DBNull.Value ? null : reader["NOTE"].ToString(),
                            LastInserted = reader["LAST_INSERTED"] == DBNull.Value ? (DateTime?)null : Convert.ToDateTime(reader["LAST_INSERTED"]),
                            LastUpdated = reader["LAST_UPDATED"] == DBNull.Value ? (DateTime?)null : Convert.ToDateTime(reader["LAST_UPDATED"]),
                            IsDeleted = reader["IS_DELETED"]?.ToString()
                        };
                    }
                }
            }
        }
        return null;
    }

    public async Task UpdateFacilityAsync(FacilityDto facility)
    {
        using (var conn = new OracleConnection(connectionString))
        {
            await conn.OpenAsync();
            string sql = @"
                UPDATE WMGMT_RCV_FACILITY SET
                    RCV_FACILITY_NAME = :name,
                    ADDRESS = :address,
                    CITY = :city,
                    RCV_FACILITY_STATE = :state,
                    ZIP = :zip,
                    PHONE = :phone,
                    FAX = :fax,
                    CONTACT_TITLE = :contactTitle,
                    CONTACT_PERSON = :contactPerson,
                    NOTE = :note,
                    LAST_UPDATED = SYSDATE
                WHERE RCV_FACILITY_ID = :id";

            using (var cmd = new OracleCommand(sql, conn))
            {
                cmd.Parameters.Add(":name", facility.FacilityName);
                cmd.Parameters.Add(":address", facility.Address);
                cmd.Parameters.Add(":city", facility.City);
                cmd.Parameters.Add(":state", facility.State);
                cmd.Parameters.Add(":zip", facility.Zip);
                cmd.Parameters.Add(":phone", string.IsNullOrEmpty(facility.Phone) ? DBNull.Value : (object)facility.Phone);
                cmd.Parameters.Add(":fax", string.IsNullOrEmpty(facility.Fax) ? DBNull.Value : (object)facility.Fax);
                cmd.Parameters.Add(":contactTitle", string.IsNullOrEmpty(facility.ContactTitle) ? DBNull.Value : (object)facility.ContactTitle);
                cmd.Parameters.Add(":contactPerson", string.IsNullOrEmpty(facility.ContactPerson) ? DBNull.Value : (object)facility.ContactPerson);
                cmd.Parameters.Add(":note", string.IsNullOrEmpty(facility.Note) ? DBNull.Value : (object)facility.Note);
                cmd.Parameters.Add(":id", facility.FacilityId);
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    public async Task DeleteFacilityAsync(int id)
    {
        using (var conn = new OracleConnection(connectionString))
        {
            await conn.OpenAsync();
            string sql = "UPDATE WMGMT_RCV_FACILITY SET IS_DELETED = 'Y', LAST_UPDATED = SYSDATE WHERE RCV_FACILITY_ID = :id";

            using (var cmd = new OracleCommand(sql, conn))
            {
                cmd.Parameters.Add(":id", id);
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    public async Task<List<FacilityDto>> GetAllFacilitiesAsync()
    {
        var facilities = new List<FacilityDto>();

        using (var conn = new OracleConnection(connectionString))
        {
            await conn.OpenAsync();
            string sql = "SELECT * FROM WMGMT_RCV_FACILITY WHERE IS_DELETED = 'N' ORDER BY RCV_FACILITY_NAME";

            using (var cmd = new OracleCommand(sql, conn))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    facilities.Add(new FacilityDto
                    {
                        FacilityId = Convert.ToInt32(reader["RCV_FACILITY_ID"]),
                        FacilityName = reader["RCV_FACILITY_NAME"]?.ToString(),
                        Address = reader["ADDRESS"]?.ToString(),
                        City = reader["CITY"]?.ToString(),
                        State = reader["RCV_FACILITY_STATE"]?.ToString(),
                        Zip = reader["ZIP"]?.ToString(),
                        Phone = reader["PHONE"] == DBNull.Value ? null : reader["PHONE"].ToString(),
                        Fax = reader["FAX"] == DBNull.Value ? null : reader["FAX"].ToString(),
                        ContactTitle = reader["CONTACT_TITLE"] == DBNull.Value ? null : reader["CONTACT_TITLE"].ToString(),
                        ContactPerson = reader["CONTACT_PERSON"] == DBNull.Value ? null : reader["CONTACT_PERSON"].ToString(),
                        Note = reader["NOTE"] == DBNull.Value ? null : reader["NOTE"].ToString(),
                        LastInserted = reader["LAST_INSERTED"] == DBNull.Value ? (DateTime?)null : Convert.ToDateTime(reader["LAST_INSERTED"]),
                        LastUpdated = reader["LAST_UPDATED"] == DBNull.Value ? (DateTime?)null : Convert.ToDateTime(reader["LAST_UPDATED"]),
                        IsDeleted = reader["IS_DELETED"]?.ToString()
                    });
                }
            }
        }

        return facilities;
    }

    public async Task<List<FacilityDto>> SearchFacilitiesAsync(string facilityName = null, string city = null, string state = null, string zip = null)
    {
        var results = new List<FacilityDto>();

        using (var conn = new OracleConnection(connectionString))
        {
            await conn.OpenAsync();
            var sql = new StringBuilder("SELECT * FROM WMGMT_RCV_FACILITY WHERE IS_DELETED = 'N'");
            var cmd = new OracleCommand();
            cmd.Connection = conn;

            if (!string.IsNullOrEmpty(facilityName))
            {
                sql.Append(" AND UPPER(RCV_FACILITY_NAME) LIKE :name");
                cmd.Parameters.Add(":name", $"%{facilityName.ToUpper()}%");
            }
            if (!string.IsNullOrEmpty(city))
            {
                sql.Append(" AND UPPER(CITY) LIKE :city");
                cmd.Parameters.Add(":city", $"%{city.ToUpper()}%");
            }
            if (!string.IsNullOrEmpty(state))
            {
                sql.Append(" AND UPPER(RCV_FACILITY_STATE) = :state");
                cmd.Parameters.Add(":state", state.ToUpper());
            }
            if (!string.IsNullOrEmpty(zip))
            {
                sql.Append(" AND ZIP = :zip");
                cmd.Parameters.Add(":zip", zip);
            }

            cmd.CommandText = sql.ToString();

            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    results.Add(new FacilityDto
                    {
                        FacilityId = Convert.ToInt32(reader["RCV_FACILITY_ID"]),
                        FacilityName = reader["RCV_FACILITY_NAME"]?.ToString(),
                        Address = reader["ADDRESS"]?.ToString(),
                        City = reader["CITY"]?.ToString(),
                        State = reader["RCV_FACILITY_STATE"]?.ToString(),
                        Zip = reader["ZIP"]?.ToString(),
                        Phone = reader["PHONE"] == DBNull.Value ? null : reader["PHONE"].ToString(),
                        Fax = reader["FAX"] == DBNull.Value ? null : reader["FAX"].ToString(),
                        ContactTitle = reader["CONTACT_TITLE"] == DBNull.Value ? null : reader["CONTACT_TITLE"].ToString(),
                        ContactPerson = reader["CONTACT_PERSON"] == DBNull.Value ? null : reader["CONTACT_PERSON"].ToString(),
                        Note = reader["NOTE"] == DBNull.Value ? null : reader["NOTE"].ToString(),
                        LastInserted = reader["LAST_INSERTED"] == DBNull.Value ? (DateTime?)null : Convert.ToDateTime(reader["LAST_INSERTED"]),
                        LastUpdated = reader["LAST_UPDATED"] == DBNull.Value ? (DateTime?)null : Convert.ToDateTime(reader["LAST_UPDATED"]),
                        IsDeleted = reader["IS_DELETED"]?.ToString()
                    });
                }
            }
        }

        return results;
    }
}

public class ReceivingFacilityController : Controller
{
    private readonly FacilityManager _manager = new FacilityManager();

    [HttpPost]
    public async Task<ActionResult> Create(FacilityDto facility)
    {
        await _manager.CreateFacilityAsync(facility);
        return Json(new { message = "Facility created successfully." });
    }

    [HttpGet]
    public async Task<ActionResult> Get(int id)
    {
        var facility = await _manager.ReadFacilityAsync(id);
        if (facility == null)
            return HttpNotFound();
        return Json(facility, JsonRequestBehavior.AllowGet);
    }

    [HttpPost]
    public async Task<ActionResult> Update(FacilityDto facility)
    {
        await _manager.UpdateFacilityAsync(facility);
        return Json(new { message = "Facility updated successfully." });
    }

    [HttpPost]
    public async Task<ActionResult> Delete(int id)
    {
        await _manager.DeleteFacilityAsync(id);
        return Json(new { message = "Facility deleted successfully." });
    }

    [HttpGet]
    public async Task<ActionResult> GetAll()
    {
        var facilities = await _manager.GetAllFacilitiesAsync();
        return Json(facilities, JsonRequestBehavior.AllowGet);
    }

    [HttpGet]
    public async Task<ActionResult> Search(string facilityName = null, string city = null, string state = null, string zip = null)
    {
        var results = await _manager.SearchFacilitiesAsync(facilityName, city, state, zip);
        return Json(results, JsonRequestBehavior.AllowGet);
    }
}
