#Secure De-Identification & Entity Identity Management Engine 
<h2>Overview</h2>
Given a few data-sets containing personal information (FirstName, MiddleName, LastName, SSN, DOB) along with other agency-specific data, the engine generates a Knowledgebase which contains all of the data aggregated such that every person has one and only one unique ID. So basically, John Doe, J. Doe and John D. should be clustered together with the same Unique ID/ Research ID. In addition to this, the engine must remove all Personally Identifiable Information (PII) that includes FirstName, MiddleName, LastName, SSN and DOB from the Knowledgebase. So every Research ID will represent 1 person. There may be several rows of data about this one person coming from different agencies, but they should all be assigned the same ResearchID.<br />

<h2>Approach</h2>
Technologies used: Java Mapreduce, Hive <br />
<ol>
<li>Add ID to each record. I’m calling this column RefID. This is for record level identification. 
<li>Perform Indexing based on user configured xml files. Java program reads xml file and feeds the values (say FirstName+MiddleName+DOB as Index 1 and SSN as Index 2) to the mapReduce jar file which runs on Hadoop and returns the index outputs. There may be any number of indexes. Indexing basically creates pre-liminary clusters based on weak rules for data matching to be faster in later stages. 
<li>Transitive closure program in Java merges clusters that have any common RefID’s. For example, If Abhishek Kumar exists in cluster 1 as per index 1 and in cluster 2 and 3 as per index 2, Transitive closure will bring clusters 1,2 and 3 together and generate the final Index results. The data handling and manipulation for Indexed data and finally Transitive closure is managed by Hive queries. 
<li>Finally, Data Matching is done within each indexed cluster using Java based on rules configured in another XML file by the user (rules similar to index rules). All those records that match against each other are assigned the same Research ID and pushed into a Knowledgebase table on Hive. The PII data is removed in the process. 
</ol>