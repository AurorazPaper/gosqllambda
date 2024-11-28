The main deliverable of this assignment along with the source code is a zip file labelled bootstrap that contains the go executable. To make changes to the lambda, compile the source code, and then zip that executable. You can then update the lambda through CLI or the AWS console interface. 

The meat of the program is just plain sql queries that are then run by the code. This hopefully makes it relatively easy for even people unfamiliar with Go to maintain and update with changes to the SQL logic.
The other hope is that the functionality can be intuited by just looking at how the file is structured.

One pitfall that is important to note is that the performance of the query is dependent on composite indexing.

Also worth noting, the code is designed for the static demo, choices were made to simplify code to make it easier to understand and to manage time constraints.
CASE should be used over coalesce in the missedCallDiff query, with a case to catch files younger than 6 hours that still don't have a match, and keep them null
This can be done by checking datetime against DATE_SUB(NOW(), INTERVAL 360 MINUTE)

Metabase settings are included in the RDS Database, making it more production ready and easier to transfer to another server or a docker container
