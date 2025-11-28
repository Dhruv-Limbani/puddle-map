from puddle_server.mcp import mcp
from puddle_server.utils import run_pg_sql
import json
from typing import Dict, Any, List

# ==========================================
# BUYER TOOLS (Chatbot -> DB)
# ==========================================

@mcp.tool(
    description="Initialize a new inquiry draft. The AI can define the initial structure of the buyer's inquiry JSON."
)
def create_buyer_inquiry(
    buyer_id: str,
    dataset_id: str,
    conversation_id: str,
    initial_state_json: Dict[str, Any]
) -> str:
    """
    Creates a new inquiry row. 
    
    Args:
        buyer_id: UUID of the buyer.
        dataset_id: UUID of the dataset.
        conversation_id: UUID of the chat session.
        initial_state_json: A generic dictionary containing the buyer's initial needs. 
                            (e.g. {"questions": ["Q1"], "status": "new", "meta": {...}})
    """
    # 1. Lookup Vendor
    vendor_sql = "SELECT vendor_id FROM datasets WHERE id = %s"
    ds_info = run_pg_sql(vendor_sql, (dataset_id,), fetch_one=True)
    
    if not ds_info:
        return "Error: Dataset not found."

    # 2. Insert with flexible JSON
    insert_sql = """
        INSERT INTO inquiries (
            buyer_id, dataset_id, vendor_id, conversation_id, 
            buyer_inquiry, status
        ) VALUES (%s, %s, %s, %s, %s, 'draft')
        RETURNING id;
    """
    
    # Ensure dict is dumped to string for SQL
    json_payload = json.dumps(initial_state_json)
    
    result = run_pg_sql(insert_sql, (
        buyer_id, dataset_id, ds_info['vendor_id'], conversation_id, 
        json_payload
    ), fetch_one=True)

    return f"Inquiry created (ID: {result['id']}). Status is 'draft'."


@mcp.tool(
    description="Update the Buyer's Inquiry JSON blob. Gives the AI full control to modify the structure or content."
)
def update_buyer_json(
    inquiry_id: str,
    new_state_json: Dict[str, Any]
) -> str:
    """
    Overwrites the 'buyer_inquiry' column with the new JSON provided.
    The AI should read the old state first, modify it, and pass the full new object here.
    """
    sql = """
        UPDATE inquiries 
        SET buyer_inquiry = %s, updated_at = NOW() 
        WHERE id = %s
    """
    run_pg_sql(sql, (json.dumps(new_state_json), inquiry_id))
    
    return "Buyer JSON state updated successfully."


@mcp.tool(
    description="Submit the inquiry to the vendor. Changes status to 'submitted'."
)
def submit_inquiry_to_vendor(inquiry_id: str) -> str:
    """
    Flags the inquiry for the Vendor Agent.
    """
    sql = """
        UPDATE inquiries 
        SET status = 'submitted', updated_at = NOW() 
        WHERE id = %s
        RETURNING status;
    """
    result = run_pg_sql(sql, (inquiry_id,), fetch_one=True)
    if result:
        return "Inquiry submitted. The Vendor Agent will now see this."
    return "Error: Inquiry not found."

# ==========================================
# SHARED / READER TOOLS
# ==========================================

@mcp.tool(
    description="Get the raw JSON states for both Buyer and Vendor. Use this to read the current negotiation status."
)
def get_inquiry_full_state(inquiry_id: str) -> str:
    """
    Returns the raw JSONs so the AI can parse and decide what to do next.
    """
    sql = """
        SELECT 
            i.status, i.buyer_inquiry, i.vendor_response,
            d.title as dataset_title, v.name as vendor_name
        FROM inquiries i
        JOIN datasets d ON i.dataset_id = d.id
        JOIN vendors v ON i.vendor_id = v.id
        WHERE i.id = %s
    """
    row = run_pg_sql(sql, (inquiry_id,), fetch_one=True)
    if not row:
        return "Inquiry not found."

    # Return as a string dump of the whole object
    return json.dumps(row, default=str)

# ==========================================
# VENDOR AGENT TOOLS (Vendor AI -> DB)
# ==========================================

@mcp.tool(
    description="Find inquiries waiting for the vendor (status='submitted')."
)
def get_vendor_work_queue(vendor_id: str) -> str:
    """
    Returns a list of inquiries that need attention.
    """
    sql = """
        SELECT i.id, d.title, i.buyer_inquiry
        FROM inquiries i
        JOIN datasets d ON i.dataset_id = d.id
        WHERE i.vendor_id = %s AND i.status = 'submitted'
    """
    results = run_pg_sql(sql, (vendor_id,))
    
    if not results:
        return "No pending inquiries."
        
    return json.dumps(results, default=str)


@mcp.tool(
    description="Update the Vendor's Response JSON. Use this to draft answers or ask clarification."
)
def update_vendor_response_json(
    inquiry_id: str,
    new_response_json: Dict[str, Any],
    mark_ready_for_review: bool = False
) -> str:
    """
    Overwrites the 'vendor_response' column.
    
    Args:
        inquiry_id: The UUID.
        new_response_json: The flexible JSON object the Vendor AI has constructed.
        mark_ready_for_review: If True, changes status to 'pending_review' (Human Alert).
                               If False, keeps status as 'submitted' (Work in Progress).
    """
    status_update = ", status = 'pending_review'" if mark_ready_for_review else ""
    
    sql = f"""
        UPDATE inquiries 
        SET vendor_response = %s, updated_at = NOW() {status_update}
        WHERE id = %s
    """
    run_pg_sql(sql, (json.dumps(new_response_json), inquiry_id))
    
    status_msg = "and sent to Human Review" if mark_ready_for_review else "as draft"
    return f"Vendor response saved {status_msg}."