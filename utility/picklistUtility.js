global.site_PickList = {};
site_PickList['source_type__c']=['Sales', 'Retailer', 'Electrician', 'Incoming'];
site_PickList['site_stages__c']=['Bricking', 'Plastering', 'Wiring', 'Installing'];
site_PickList['project_type__c']=['Residential', 'School', 'Healthcare', 'Retail', 'Factory'];
site_PickList['status__c']=['Funnel', 'Lost', 'Won', 'Rejected','',null];

global.event_PickList = {};
event_PickList['status__c']=['Draft', 'Pending for Approval', 'Approved', 'Rejected', 'Completed'];
event_PickList['target_audience__c']=['Retailers', 'Electricians'];
event_PickList['type__c']=['nukkad meet', 'electrician meet', 'retailer meet', 'commando activity', 'roadshow', 'umbrella activity'];

global.influencers_PickList = {};
influencers_PickList['category__c']=['electricians'];
influencers_PickList['potential__c']=['Low', 'Medium', 'High'];
influencers_PickList['status__c']=['Active', 'Inactive'];

global.visit_PickList = {};
visit_PickList['status__c']=['Completed', 'Reschedule', 'Cancelled', 'Open', 'Unexecuted', 'Started'];
visit_PickList['send_marketing_material__c']=['Yes', 'No'];

visit_PickList['visibility_level__c']=['Low', 'Medium','High'];
visit_PickList['Assigned_by__c']=['Self', 'Manager'];
visit_PickList['Approval_Status__c']=['Pending for Approval', 'Approved','Rejected','Draft'];
visit_PickList['Issue_Type__c']=[];
visit_PickList['Issue_Sub_Type__c']=['Wrong Supply', 'Short Supply','Transit Damage','Mishandling','Out of Warranty','Customer end failure','Wrong selection of Product','Manufacturing Defect'];
visit_PickList['Location_Matched__c']=[];
visit_PickList['Resolution_Type__c']=[];
visit_PickList['Visit_Mark_Complete__c']=[];

global.attendance_PickList = {};
attendance_PickList['absent_reason__c']=['Planned','Ad-hoc'];
attendance_PickList['type__c']=['Absent','Present'];

global.seller_Category_PickValues = ['A', 'B', 'C'];