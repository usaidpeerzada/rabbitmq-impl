const amqp = require('amqplib/callback_api');
const axios = require('axios');
const CSVToJSON = require('csvtojson');
const fs = require('fs');
const jsonData = require('./logs.json');

require('dotenv').config();

// convert csv file to JSON
CSVToJSON().fromFile('log.csv')
.then(logs => {
    fs.writeFile('logs.json', JSON.stringify(logs, null, 4), (err) => {
        if (err) {
            throw err;
        } else {
            console.log('\x1b[32m%s\x1b[0m', 'JSON File Created!')
        }
    });
    
}).catch(err => {
    // log error if any
    console.log(err);
});

// if the connection is closed or fails to be established at all, we will reconnect
var amqpConn = null;
function start() {
  amqp.connect(process.env.AMQ_INSTANCE_URL, function(err, conn) {
    if (err) {
      console.error("[AMQP]", err.message);
      return setTimeout(start, 1000);
    }
    conn.on("error", function(err) {
      if (err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message);
      }
    });
    conn.on("close", function() {
      console.error("[AMQP] reconnecting");
      return setTimeout(start, 1000);
    });

    console.log("[AMQP] connected");
    amqpConn = conn;

    whenConnected();
  });
}

function whenConnected() {
  startPublisher();
  startWorker();
}

var pubChannel = null;
var offlinePubQueue = [];
function startPublisher() {
  amqpConn.createConfirmChannel(function(err, ch) {
    if (closeOnErr(err)) return;
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed");
    });

    pubChannel = ch;
    while (true) {
      var m = offlinePubQueue.shift();
      if (!m) break;
      publish(m[0], m[1], m[2]);
    }
  });
}

// method to publish a message, will queue messages internally if the connection is down and resend later
function publish(exchange, routingKey, content) {
  try {
    pubChannel.publish(exchange, routingKey, content, { persistent: true },
                       function(err, ok) {
                         if (err) {
                           console.error("[AMQP] publish", err);
                           offlinePubQueue.push([exchange, routingKey, content]);
                           pubChannel.connection.close();
                         }
                       });
  } catch (e) {
    console.error("[AMQP] publish", e.message);
    offlinePubQueue.push([exchange, routingKey, content]);
  }
}

// A worker that acks messages only if processed succesfully
function startWorker() {
  amqpConn.createChannel(function(err, ch) {
    if (closeOnErr(err)) return;
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed");
    });
    ch.prefetch(10);
    ch.assertQueue("tripeasyMobileApp", { durable: true }, function(err, _ok) {
      if (closeOnErr(err)) return;
      ch.consume("tripeasyMobileApp", processMsg, { noAck: false });
      console.log("Worker is started");
    });

    function processMsg(msg) {
      work(msg, function(ok) {
        try {
          if (ok)
            ch.ack(msg);
          else
            ch.reject(msg, true);
        } catch (e) {
          closeOnErr(e);
        }
      });
    }
});
}

function work(msg, cb) {
    console.dir(msg?.content?.toString(), {'maxArrayLength': 10000});
  cb(true);
}

function closeOnErr(err) {
  if (!err) return false;
  console.error("[AMQP] error", err);
  amqpConn.close();
  return true;
}

function getMessagesFromTez() {
    // let msg = stringNew ?  JSON.stringify(stringNew) : "waiting..."; 
    let msg = 'JSON.stringify(jsonData)'
    return msg
}


start();

setInterval(function() {
    publish("", "tripeasyMobileApp", new Buffer.from(getMessagesFromTez()));
}, 3000);