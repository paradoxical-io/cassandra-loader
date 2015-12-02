
CREATE TABLE request_status (
    tracking_id uuid PRIMARY KEY,
    callback_routing_key text,
    created_date timestamp,
    api_id bigint,
    request_type text,
    response text,
    status text,
    updated_date timestamp
);