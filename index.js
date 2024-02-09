const {Storage} = require('@google-cloud/storage');
const {BigQuery} = require('@google-cloud/bigquery');

const csv = require('csv-parser');
const bigquery = new BigQuery();
const datasetId = 'weather_etl';
const tableId = 'indyObservations';

const path = require('path');

exports.readObservation = (file, context) => {
    const gcs = new Storage();
    const dataFile = gcs.bucket(file.bucket).file(file.name);

    //pull the file name and store into var
    //if i want to keep the .0 i can add csv to the split
    const fileName = path.basename(file.name);
    const station = fileName.split('.')[0];

    dataFile.createReadStream()
    .on('error', () => {
        console.log(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        // console.log(row);
        row = transform(row);

        //printDict(row);
        writeToBQ(row, station);
    })
    .on('end', () => {
        console.log('end of file');
    });
}


// HELPER FUNCTIONS
// function printDict(row) {
//     for (let key in row) {
//         //helps print each row 
//         console.log(key + ' : ' + row[key]);
//     }
// }

function transform(row) {
    //helps convert the strings into numbers with Number()
    for (let key in row) {

        if (row[key] === '-9999.0'){
            //replaces the -9999 with null a bit annoying trying to filter
            row[key] = null;

        } else if (['airtemp', 'dewpoint', 'pressure', 'windspeed', 'precip1hour', 'precip6hour']
        .includes(key)) {
            // Divide values by 10 and rewrite null values as null
            //easier to follow for me this way
            row[key] = row[key] !== null ? Number(row[key]) / 10 : null;

        } else {
            //everything else is listed per Number()
            row[key] = Number(row[key]);
        }
    }
    return row
}


//create a helper function that writes to BQ
//function must be asynchronus
async function writeToBQ(row, station) {
    //adding station number to the row i hope
    row.station = station;

    // BQ expects an array of rows
    var rows = []; //Empty array
    rows.push(row);

    // Insert data into the indyObservations table
    await bigquery
    .dataset(datasetId)
    .table(tableId)
    .insert(rows)
    .then((foundErrors) => {
        rows.forEach((row) => console.log('Inserted: ', row));
    })
    .catch((err) => {
        console.error ('ERROR:', err);
    })
}