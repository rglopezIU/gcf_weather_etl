const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');

exports.readObservation = (file, context) => {
    // console.log(`  Event: ${context.eventId}`);
    // console.log(`  Event Type: ${context.eventType}`);
    // console.log(`  Bucket: ${file.bucket}`);
    // console.log(`  File: ${file.name}`);

    //new instance of object storage
    const gcs = new Storage();

    //open file and list off the details
    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .on('error', () => {
        //to handle the error
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        //Log row data
        console.log(row);
    })
    .on('end', () => {
        //Handle end of CSV
        console.log('End!');
    });

}