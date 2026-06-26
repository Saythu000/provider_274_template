from typing import Union, List, Dict
from .mappings import HIERARCHY_PROVIDER_MAPPING_DEFINITION


class CSVSchemaMapper:
    """
    Maps EDI structured JSON to CSV-friendly flat dictionaries for Providers.
    Focuses exclusively on Provider Hierarchy records from Directory (EDI 274).
    """

    def __init__(self):
        pass

    # ------------------------------------------------------------------
    # Provider Hierarchy Mapping (EDI 274 Directory)
    # ------------------------------------------------------------------

    def map_hierarchy(self, structured_json: dict) -> List[Dict]:
        """
        Map EDI 274 Directory structured JSON to a list of provider hierarchy
        CSV records matching provider_hierarchy_7.12_schema.json columns.

        Produces three record types:
          1. Rendering Provider location record.
          2. Rendering-to-Billing link record.
          3. Billing Provider location record.

        Args:
            structured_json: Single structured 274 dict.

        Returns:
            List of hierarchy record dictionaries.
        """
        records = []
        try:
            # Extract segments from the interchange_control_header_loop
            loop = structured_json.get("detail", {}).get("interchange_control_header_loop", {})
            nm1_list = loop.get("interchange_control_header_NM1", [])
            n3_obj   = loop.get("interchange_control_header_N3", {})
            n4_obj   = loop.get("interchange_control_header_N4", {})
            ref_list = loop.get("interchange_control_header_REF", [])
            per_list = loop.get("interchange_control_header_PER", [])
            dtp_list = loop.get("interchange_control_header_DTP", [])

            # Extract provider info from NM1[2] (1P entity = provider)
            provider_nm1 = nm1_list[2] if len(nm1_list) > 2 else {}
            provider_id   = provider_nm1.get("identifier", "")
            provider_name = provider_nm1.get("name", "")
            
            # Extract address info (N3 and N4 are objects, not arrays)
            addr_line1 = n3_obj.get("address_line_1", "")
            addr_line2 = n3_obj.get("address_line_2", "")
            city       = n4_obj.get("city", "")
            state      = n4_obj.get("state", "")
            zip_code   = n4_obj.get("zip_code", "")
            
            # Extract TIN from REF[0]
            tin = ref_list[0].get("champus_id", "") if ref_list else ""
            
            # Extract contact info from PER[1]
            phone = per_list[1].get("per04_04", "") if len(per_list) > 1 else ""
            contact = per_list[1].get("per02_02", "") if len(per_list) > 1 else ""

            # Extract dates from DTP segments
            start_date = dtp_list[0].get("date_value", "01/01/2026") if dtp_list else "01/01/2026"
            end_date   = dtp_list[1].get("date_value", "12/31/2026") if len(dtp_list) > 1 else "12/31/2026"
            
            # Format dates to MM/DD/YYYY
            def format_date(date_str):
                if len(date_str) == 8:  # YYYYMMDD format
                    return f"{date_str[4:6]}/{date_str[6:8]}/{date_str[0:4]}"
                return date_str
            
            start_date = format_date(start_date)
            end_date = format_date(end_date)

            # 1. Rendering Provider record
            records.append({
                "TEMPLATE": "TEMPLATE", "PROVIDERID": provider_id, "PROVIDERLASTNAME": provider_name,
                "PROVIDERNPI": provider_id, "LOCATIONGROUPID": "G1", "LOCATIONRANKING": 1,
                "LOCATIONIDTYPE": "rendering", "LOCATIONID": "L1", "LOCATIONDESC": provider_name,
                "LOCATIONTIN": tin, "LOCATIONADDRESS1": addr_line1, "LOCATIONCITY": city,
                "LOCATIONSTATE": state, "LOCATIONZIP": zip_code,
                "PHONENUMBER": phone, "CONTACTPERSON": contact,
                "STARTDATE": start_date, "ENDDATE": end_date
            })

            # 2. Rendering-to-Billing link
            records.append({
                "TEMPLATE": "TEMPLATE", "PROVIDERID": provider_id, "PROVIDERLASTNAME": provider_name,
                "PROVIDERNPI": provider_id, "LOCATIONGROUPID": "G1", "LOCATIONRANKING": 1,
                "LOCATIONIDTYPE": "rendering to billing", "LOCATIONID": provider_id,
                "LOCATIONDESC": provider_name,
                "LOCATIONTIN": tin, "LOCATIONADDRESS1": addr_line1, "LOCATIONCITY": city,
                "LOCATIONSTATE": state, "LOCATIONZIP": zip_code,
                "PHONENUMBER": phone, "CONTACTPERSON": contact,
                "STARTDATE": start_date, "ENDDATE": end_date
            })

            # 3. Billing Provider record
            records.append({
                "TEMPLATE": "TEMPLATE", "PROVIDERID": provider_id, "PROVIDERLASTNAME": provider_name,
                "PROVIDERNPI": provider_id, "LOCATIONGROUPID": "G1", "LOCATIONRANKING": 1,
                "LOCATIONIDTYPE": "billing", "LOCATIONID": provider_id,
                "LOCATIONDESC": provider_name, "LOCATIONTIN": tin,
                "LOCATIONADDRESS1": addr_line1, "LOCATIONCITY": city,
                "LOCATIONSTATE": state, "LOCATIONZIP": zip_code,
                "PHONENUMBER": phone, "CONTACTPERSON": contact,
                "STARTDATE": start_date, "ENDDATE": end_date
            })

        except Exception as e:
            print(f"Warning: Failed to map provider hierarchy: {e}")

        return records
