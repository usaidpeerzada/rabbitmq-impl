#!/usr/bin/env node

var amqp = require('amqplib/callback_api');
require('dotenv').config();

amqp.connect(process.env.AMQ_INSTANCE_URL_LOCAL, function(error0, connection) {
    if (error0) {
        throw error0;
    }
    connection.createChannel(function(error1, channel) {
        if (error1) {
            throw error1;
        }

        var queue = 'tripeasyMobile';
        var msg = 'This is a test message for tripeasy mobile!';

        channel.assertQueue(queue, {
            durable: false
        });
        channel.sendToQueue(queue, Buffer.from(msg));

        console.log(" [x] Sent %s", msg);
    });
    setTimeout(function() {
        connection.close();
        process.exit(0);
    }, 500);
});