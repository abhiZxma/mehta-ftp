const admin = require('firebase-admin');
const qry = require(`${PROJECT_DIR}/utility/selectQueries`);
const dtUtil = require(`${PROJECT_DIR}/utility/dateUtility`);

const serviceAccount = require('../century-ply-24cf2-firebase-adminsdk-a3z87-99f4487493.json');


admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://push-notifications-65c64.firebaseio.com',
});

module.exports = {
  sendnotificationToDevice,
  sendNotificationToMultipleDevices,
};

async function sendnotificationToDevice(pushNotification, notification, data = {}) {
  try {
      const { user__c, firebase_token__c,title,body } = pushNotification;
      const res = await admin.messaging().sendToDevice(firebase_token__c, { notification, data });   
    if (res.successCount) {
      let tableName = SF_NOTIFICATIONS_TABLE_NAME;
      let notifications = `team__c, name, body__c, createddate`;
      let notificationsValues = [user__c, title, body, dtUtil.todayDatetime()];
      qry.insertRecord(notifications, notificationsValues, tableName);
      console.log('Push notifiction sent successfully!');
    } else {
      console.log('Failed to send notifiction', res.results[0].error);
    }
  } catch (err) {
    console.log('Error in sendnotificationToDevice:', err);
  }
}

async function sendNotificationToMultipleDevices(tokens, notification, data = {}) {
  try {
    const res = await admin.messaging().sendMulticast({ tokens, notification, data });
    if (res.successCount) {
      console.log('Push notifiction sent successfully!');
    } else {
      console.log('Failed to send notifiction');
    }
  } catch (err) {
    console.log('Error in sendnotificationToDevice:', err);
  }
}