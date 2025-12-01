from puddle_server.mcp import mcp
from puddle_server.utils import run_pg_sql
import json
from typing import Dict, Any, List

# ==========================================
# BUYER TOOLS (Chatbot -> DB)
# ==========================================

@mcp.tool(
    description="Initialize a new inquiry and submit it to vendor. The AI can define the initial structure of the buyer's inquiry JSON and provide an initial summary."
)
def create_buyer_inquiry(
    buyer_id: str,
    dataset_id: str,
    conversation_id: str,
    initial_state_json: Dict[str, Any],
    initial_summary: str
) -> str:
    """
    Creates a new inquiry row with 'submitted' status.
    
    Args:
        buyer_id: UUID of the buyer.
        dataset_id: UUID of the dataset.
        conversation_id: UUID of the chat session.
        initial_state_json: A generic dictionary containing the buyer's initial needs. 
                            (e.g. {"questions": ["Q1"], "status": "new", "meta": {...}})
        initial_summary: AI-generated NARRATIVE summary (past tense) describing what the buyer requested.
                        Example: "The buyer expressed interest in X dataset and was concerned about Y. They mentioned budget Z."
    """
    # 1. Lookup Vendor
    vendor_sql = "SELECT vendor_id FROM datasets WHERE id = %s"
    ds_info = run_pg_sql(vendor_sql, (dataset_id,), fetch_one=True)
    
    if not ds_info:
        return "Error: Dataset not found."

    # 2. Insert with flexible JSON and summary
    insert_sql = """
        INSERT INTO inquiries (
            buyer_id, dataset_id, vendor_id, conversation_id, 
            buyer_inquiry, summary, status
        ) VALUES (%s, %s, %s, %s, %s, %s, 'submitted')
        RETURNING id;
    """
    
    # Ensure dict is dumped to string for SQL
    json_payload = json.dumps(initial_state_json)
    
    result = run_pg_sql(insert_sql, (
        buyer_id, dataset_id, ds_info['vendor_id'], conversation_id, 
        json_payload, initial_summary
    ), fetch_one=True)

    return f"Inquiry created and submitted to vendor (ID: {result['id']}). Status is 'submitted'."


@mcp.tool(
    description="Update the Buyer's Inquiry JSON blob and append to the historical summary narrative. CRITICAL: You MUST first call get_inquiry_full_state to get the existing summary, then append your new text to it."
)
def update_buyer_json(
    inquiry_id: str,
    new_state_json: Dict[str, Any],
    updated_summary: str
) -> str:
    """
    Overwrites the 'buyer_inquiry' column with the new JSON provided and updates the summary.
    
    CRITICAL WORKFLOW:
    1. FIRST call get_inquiry_full_state to get the existing summary
    2. Modify the buyer_inquiry JSON
    3. Take the ENTIRE existing summary text
    4. APPEND new sentence(s) describing this change to the END of existing summary
    5. Pass the COMPLETE cumulative text (old + new) as updated_summary
    
    Example: 
    - Existing: "Buyer asked for X with budget Y."
    - New change: Buyer adds requirement for region Z
    - updated_summary param: "Buyer asked for X with budget Y. Buyer then added requirement for region Z."
    
    WARNING: If updated_summary is shorter than existing, the update will FAIL.
    """
    # Get existing summary to validate
    check_sql = "SELECT summary FROM inquiries WHERE id = %s"
    existing = run_pg_sql(check_sql, (inquiry_id,), fetch_one=True)
    
    if existing and existing.get('summary'):
        existing_summary = existing['summary']
        # Validate that new summary contains the old one (basic check)
        if existing_summary and existing_summary not in updated_summary:
            return f"ERROR: The updated_summary must CONTAIN the entire existing summary. You provided a summary that doesn't include the existing text. EXISTING SUMMARY: '{existing_summary}'. Please call get_inquiry_full_state, read the existing summary, and APPEND to it."
    
    sql = """
        UPDATE inquiries 
        SET buyer_inquiry = %s, summary = %s, updated_at = NOW() 
        WHERE id = %s
    """
    run_pg_sql(sql, (json.dumps(new_state_json), updated_summary, inquiry_id))
    
    return "Buyer JSON state and summary updated successfully."


@mcp.tool(
    description="Re-submit the inquiry to the vendor after modifications. Changes status back to 'submitted' from 'responded'."
)
def resubmit_inquiry_to_vendor(inquiry_id: str) -> str:
    """
    Re-flags the inquiry for the Vendor Agent after buyer makes changes to a responded inquiry.
    This changes status from 'responded' back to 'submitted'.
    """
    sql = """
        UPDATE inquiries 
        SET status = 'submitted', updated_at = NOW() 
        WHERE id = %s AND status = 'responded'
        RETURNING status;
    """
    result = run_pg_sql(sql, (inquiry_id,), fetch_one=True)
    if result:
        return "Inquiry re-submitted. The Vendor Agent will now see the updated inquiry."
    return "Error: Inquiry not found or not in 'responded' status."

# ==========================================
# SHARED / READER TOOLS
# ==========================================

@mcp.tool(
    description="Get the raw JSON states for both Buyer and Vendor, including the cumulative historical summary. Use this to read the full negotiation story."
)
def get_inquiry_full_state(inquiry_id: str) -> str:
    """
    Returns the raw JSONs and summary so the AI can parse and decide what to do next.
    When updating either buyer_inquiry or vendor_response, the AI should:
    1. Read this full state (especially the existing summary - the story so far)
    2. Make the changes to the appropriate JSON
    3. Generate a new summary by APPENDING to the existing one (keep 100% of old text, add new development)
    4. Update with the new JSON and cumulative summary
    
    CRITICAL: The summary field contains a NARRATIVE HISTORY. Never replace it - always append to it.
    """
    sql = """
        SELECT 
            i.status, i.buyer_inquiry, i.vendor_response, i.summary,
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


# ==========================================
# BUYER RESPONSE TOOLS (Final Actions)
# ==========================================

@mcp.tool(
    description="Accept the vendor's response and finalize the deal. Changes status to 'accepted'."
)
def accept_vendor_response(
    inquiry_id: str,
    final_notes: str = ""
) -> str:
    """
    Buyer accepts the vendor's response. This marks the inquiry as 'accepted' (deal done).
    
    Args:
        inquiry_id: The UUID of the inquiry.
        final_notes: Optional notes from the buyer about acceptance.
    """
    # Get current state to append to summary
    current_state = get_inquiry_full_state(inquiry_id)
    if "Inquiry not found" in current_state:
        return current_state
    
    state_data = json.loads(current_state)
    existing_summary = state_data.get('summary', '')
    
    # Append acceptance to summary
    acceptance_note = f"\n\nDEAL ACCEPTED by buyer. {final_notes if final_notes else 'No additional notes.'}"
    new_summary = existing_summary + acceptance_note
    
    sql = """
        UPDATE inquiries 
        SET status = 'accepted', summary = %s, updated_at = NOW()
        WHERE id = %s AND status = 'responded'
        RETURNING status;
    """
    result = run_pg_sql(sql, (new_summary, inquiry_id), fetch_one=True)
    
    if result:
        return "Inquiry accepted! Deal finalized. The vendor will be notified."
    return "Error: Inquiry not found or not in 'responded' status."


@mcp.tool(
    description="Reject the vendor's response. Changes status to 'rejected'."
)
def reject_vendor_response(
    inquiry_id: str,
    rejection_reason: str
) -> str:
    """
    Buyer rejects the vendor's response. This marks the inquiry as 'rejected' (deal lost).
    
    Args:
        inquiry_id: The UUID of the inquiry.
        rejection_reason: Reason for rejection (required for vendor feedback).
    """
    # Get current state to append to summary
    current_state = get_inquiry_full_state(inquiry_id)
    if "Inquiry not found" in current_state:
        return current_state
    
    state_data = json.loads(current_state)
    existing_summary = state_data.get('summary', '')
    
    # Append rejection to summary
    rejection_note = f"\n\nDEAL REJECTED by buyer. Reason: {rejection_reason}"
    new_summary = existing_summary + rejection_note
    
    sql = """
        UPDATE inquiries 
        SET status = 'rejected', summary = %s, updated_at = NOW()
        WHERE id = %s AND status = 'responded'
        RETURNING status;
    """
    result = run_pg_sql(sql, (new_summary, inquiry_id), fetch_one=True)
    
    if result:
        return "Inquiry rejected. The vendor will be notified."
    return "Error: Inquiry not found or not in 'responded' status."


@mcp.tool(
    description="Update the Vendor's Response JSON and append to the historical summary narrative. Changes status to 'responded'. CRITICAL: You MUST first call get_inquiry_full_state to get the existing summary, then append your new text to it."
)
def update_vendor_response_json(
    inquiry_id: str,
    new_response_json: Dict[str, Any],
    updated_summary: str
) -> str:
    """
    Overwrites the 'vendor_response' column, updates summary, and changes status to 'responded'.
    
    CRITICAL WORKFLOW:
    1. FIRST call get_inquiry_full_state to get the existing summary
    2. Construct the vendor response JSON
    3. Take the ENTIRE existing summary text
    4. APPEND new sentence(s) describing the vendor's response to the END
    5. Pass the COMPLETE cumulative text (old + new) as updated_summary
    
    Example:
    - Existing: "Buyer requested real-time data with budget $5k."
    - Vendor responds with counter offer
    - updated_summary param: "Buyer requested real-time data with budget $5k. Vendor confirmed availability but counter-offered at $7k due to API costs."
    
    WARNING: If updated_summary is shorter than existing, the update will FAIL.
    """
    # Get existing summary to validate
    check_sql = "SELECT summary FROM inquiries WHERE id = %s"
    existing = run_pg_sql(check_sql, (inquiry_id,), fetch_one=True)
    
    if existing and existing.get('summary'):
        existing_summary = existing['summary']
        # Validate that new summary contains the old one
        if existing_summary and existing_summary not in updated_summary:
            return f"ERROR: The updated_summary must CONTAIN the entire existing summary. You provided a summary that doesn't include the existing text. EXISTING SUMMARY: '{existing_summary}'. Please call get_inquiry_full_state, read the existing summary, and APPEND to it."
    
    sql = """
        UPDATE inquiries 
        SET vendor_response = %s, summary = %s, status = 'responded', updated_at = NOW()
        WHERE id = %s
    """
    run_pg_sql(sql, (json.dumps(new_response_json), updated_summary, inquiry_id))
    
    return "Vendor response and summary updated. Status changed to 'responded' - buyer will be notified."