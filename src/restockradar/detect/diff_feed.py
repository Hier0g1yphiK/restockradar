"""New-product detection vs ``seen_products`` with keyword filtering (Req 3, 12).
Always records ``seen_products`` regardless of include/exclude/alert, and suppresses
the new-product alert when the product matches an enabled Listing on the same retailer
by external_id OR handle (the Listing already covers it).
"""
