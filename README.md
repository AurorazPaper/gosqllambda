The main deliverable of this assignment along with the source code is a zip file labelled bootstrap that contains the go executable. To make changes to the lambda, compile the source code, and then zip that executable. You can then update the lambda through CLI or the AWS user interface. 

The meat of the program is just plain sql queries that are then run by the code. This hopefully makes it relatively easy for even people unfamiliar with Go to maintain and update with changes to the SQL logic.
The other hope is that the functionality can be intuited by just looking at how the file is structured.

One pitfall that is important to note is that the performance of the query is dependent on composite indexing
