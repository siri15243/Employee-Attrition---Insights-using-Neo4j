* LOAD CSV WITH HEADERS FROM "https://gist.githubusercontent.com/jexp/e329b8cb8fdb9176ae67991d3e7d4941/raw/cfc45ebd9c35a020cb6b45cbc453171a87fdc1cc/employee-attrition.csv " AS row
with  linenumber()-1 as id,row
Create (e:Employee {id:id})
SET e += row

* MATCH (e:Employee)
MERGE (d:Department {name:e.Department})
MERGE (e)-[r:WORKS_IN]->(d);

* MATCH (n:Department) RETURN n.name as Department_Names

* MATCH (e:Employee)
WHERE e.Attrition = 'Yes'
SET e:Job_Quited;

* MATCH (n:Job_Quited) RETURN n LIMIT 25

* MATCH p = (j:Job_Quited)-[]-(d:Department)
RETURN p LIMIT 4

* match (e:Employee)
merge (d:BusinessTravel {name:e.BusinessTravel})
merge (e)-[r:TRAVEL]->(d);


* MATCH (e:Employee)
CREATE (j:Job {name:e.JobRole})
SET j.JobSatisfaction=toInteger(e.JobSatisfaction),
    j.JobInvolvement = toInteger(e.JobInvolvement),
    j.JobLevel = toInteger(e.JobLevel),
    j.MonthlyIncome = toInteger(e.MonthlyIncome)
MERGE (e)-[r:WORKS_AS]->(j);

* match (e:Employee)
merge (edu:Education {name:e.Higher_Education})
merge (e)-[r:HAS_DEGREE]->(edu);

* MATCH (edu:Education)
RETURN edu.name, size( (edu)<--() ) as employee_count
ORDER BY employee_count  DESC;

* match (e:Employee)
merge (d:MaritalStatus {name:e.MaritalStatus})
merge (e)-[r:MARITAL_STATUS]->(d);

* match(e:Employee) match(j:Job)
with Distinct e.JobRole as job, j,
count( e.MonthlyIncome) as c, collect(toInteger(e.MonthlyIncome)) as income, sum(toInteger(e.MonthlyIncome)) as s
set (CASE WHEN j.name = job THEN j END).averageIncome = toFloat(s)/toFloat(c)
return job,c,toFloat(s)/toFloat(c) as averageIncome, s,income

* match(n:Employee) match(m:Job)
where n.JobRole = m.name
set n.averageIncome = m.averageIncome

* match(n:Employee)
where n.averageIncome < toFloat(n.MonthlyIncome)
set n.avgInc = 1 

* match(n:Employee)
where n.averageIncome > toFloat(n.MonthlyIncome)
set n.avgInc = 0

* MATCH (e:Employee)
WHERE e.avgInc = 0
SET e:avgInc_0;

*   MATCH (e:Employee)
WHERE e.avgInc = 1
SET e:avgInc_1;

* match (n:Employee)
with distinct n.Attrition as Attrition_Category,
count(n) as Employee_Count, n.avgInc as avg
return Attrition_Category, Employee_Count, avg 

* match (n:Employee)
where n.Attrition = 'Yes'
return n.Attrition as Attrition, count(n) as Employee_Count, n.avgInc as avg, n.JobSatisfaction as JobSatisfaction

* match (n:Employee) 
where n.Attrition = "Yes"
return  count(n) as Employee_Count , n.Leaves as Leaves
order by Leaves desc
