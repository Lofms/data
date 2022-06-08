// ./src/index.js

// importing the dependencies
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');var fs = require('fs');
var jsonl = require("jsonl");
const { stringify } = require('querystring');
const axios = require('axios');


var mysql = require('mysql');

var con = mysql.createConnection({
  host: process.env.db_host,
  user: process.env.db_user,
  password: process.env.db_password,
  database: process.env.database
});


con.connect(function(err) {
  if (err) throw err;
  console.log("Connected!");
  
});


// defining the Express app
const app = express();




var stream = fs.createReadStream('./src/2020.jsonl', {flags: 'r', encoding: 'utf-8'});
var buf = '';

stream.on('end', () => {
  console.log(counter)
  console.log('There will be no more data.');
});

stream.on('data', function(d) {
    buf += d.toString(); // when data is read, stash it in a string buffer
    pump(); // then process the buffer
});
let occations = 0
function pump() {
    var pos;

    while ((pos = buf.indexOf('\n')) >= 0) { // keep going while there's a newline somewhere in the buffer
        if (pos == 0) { // if there's more than one newline in a row, the buffer will now start with a newline
            buf = buf.slice(1); // discard it
            continue; // so that the next iteration will start with data
        }
        processLine(buf.slice(0,pos)); // hand off the line
        buf = buf.slice(pos+1); // and slice the processed data off the buffer
    }

}
var counter = 0
function processLine(line) { // here's where we do something with a line

    if (line[line.length-1] == '\r') line=line.substr(0,line.length-1); // discard CR (0x0D)
  
    if (line.length > 0) { // ignore empty lines
        var obj = JSON.parse(line); // parse the JSON
        

        
        if (obj.keywords.enriched != null){
          axios.get(`https://taxonomy.api.jobtechdev.se/v1/taxonomy/specific/concepts/municipality?lau-2-code-2015=${obj.workplace_address.municipality_code}`)
          .then(response => {
          
            obj.keywords.enriched.skill.forEach(element => {
              counter++
              

              
              var sql = `INSERT INTO db_historicalskills (municipality_label, municipality_concept_id, municipality_code, occupation_group_label, occupation_group_concept_id,occupation_field_label, occupation_field_concept_id, skill ,vacancies , year, publication_date, last_publication_date)
               VALUES ( '${response.data[0]['taxonomy/preferred-label']}', '${response.data[0]["taxonomy/id"]}', '${obj.workplace_address.municipality_code}', '${obj.occupation_group[0].label}', '${obj.occupation_group[0].concept_id}', '${obj.occupation_field[0].label}', '${obj.occupation_field[0].concept_id}', '${element}', '${Number(obj.number_of_vacancies)}', '${obj.publication_date.slice(0,4)}', '${obj.publication_date.slice(0,10)}', '${obj.last_publication_date.slice(0,10)}');`;
              con.query(sql, function (err, result) {
                if (err) throw err;
                
              });
              
              
            });
          })
          .catch(error => {
            
            
            
          });
          

      }
        
        
    }
}



// defining an array to work as the database (temporary solution)


// adding Helmet to enhance your API's security
app.use(helmet());

// using bodyParser to parse JSON bodies into JS objects
app.use(bodyParser.json());

// enabling CORS for all requests
app.use(cors());

// adding morgan to log HTTP requests

app.use(morgan('combined'));



// defining an endpoint to return all ads
app.get('/2021/', async (req, res) => {
    let municipality = req.query.municipality;
   
    
    


});

// starting the server
app.listen(3003, () => {
  console.log('listening on port 3001');
});