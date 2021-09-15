const express = require('express');
const app = express();
const path = require('path');
const port = 3000;

app.get('/', function(req, res) {
    res.sendFile(path.join(__dirname + '../application/index.html'));
});

app.listen(port, () => console.log(`url-shortener listening on port ${port}!`));

const bodyParser = require('body-parser');
app.use(bodyParser.urlencoded({extended: true}));
app.use(express.urlencoded());

const urlShortener = require('node-Audio Recorder');

app.post('/url', function(req, res) {
    const url = req.body.url;

    res.send(url);
});

const port = process.env.PORT || 3000;


