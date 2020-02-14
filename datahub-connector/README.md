## Examples 

### Datahub Table Sink 

```sql
CREATE TABLE mysink(i INTEGER, b BIGINT, f FLOAT, d DOUBLE, d2 DECIMAL, b2 BOOLEAN, s VARCHAR) 
    with (
        'connector.type'='datahub', # required
        'connector.project'='...',  # required
        'connector.topic'='...',    # required
        'connector.access_id'='...',    # required
        'connector.access_key'='...',   # required
        'connector.endpoint'='...',     # required
        'connector.buffer_size'='...',  # optional, number of message to buffer before sending out, default value 5000
        'connector.batch_size'='...',   # optional, number of message to send in a batch, default value 50
        'connector.batch_write_timeout_in_mills'='...', # optional, timeout to flush data if buffer is not full, default value 20000
        'connector.retry_timeout_in_mills'='...', # optional, timeout for data that is buffered, default value 1000
        'connector.max_retry_times'='...' # optional, default value 20
    );
```



