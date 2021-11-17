'use strict';
const async = require('async');
const _ = require('underscore');

const database = require('./lib/database');

const conNames = [
    /*'Intercon D',
    'Intercon E',
    'Intercon F',
    'Intercon G',
    'Intercon H',
    'Intercon I',
    'Intercon J',
    'Intercon K',
    'Intercon L',
    'Intercon M',
    'Intercon N',
    'Intercon O',
    'Intercon P',
    'Intercon Q',
    'Intercon R',
    'Intercon S',*/
    'Intercon T',
    'Intercon U',
];

function end(err, result){
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(JSON.stringify(result, null, 2));
    process.exit(0);
}

const output = {};

async.eachSeries(conNames, function(conName, cb){
    console.log('working on ' + conName);
    getConventionId(conName, function(err, conId){
        if (err) { return cb(err); }
        async.parallel({
            events: function(cb){
                getEventCount(conId, cb);
            },
            runs: function(cb){
                getRunCount(conId, cb);
            },
            slots: function(cb){
                getCountedSlots(conId, cb);
            },
            npcSignups: function(cb){
                getEarlySignups(conId, cb);
            }
        }, function(err, data){
            if (err) { return cb(err); }
            output[conName] = data;
            cb();
        });
    });
}, function(err){
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(JSON.stringify(output, null, 2));
    process.exit(0);
});

function getConventionId(name, cb){
    const select = 'select id from conventions where name = $1';
    database.query(select, [name], function(err, result){
        if (err) { return cb(err); }
        cb(null, result.rows[0].id);
    });
}

function getEventCount(conId, cb){
    let select = 'select count(*) as cnt from events e ';
    select += 'left join event_categories ec on e.event_category_id = ec.id ';
    select += 'where upper(ec.name) = \'LARP\' and e.status = \'active\' and e.convention_id = $1 ';
    select += exemptGames();

    database.query(select, [conId], function(err, result){
        if (err) { return cb(err); }
        cb(null, result.rows[0].cnt);
    });
}

function getRunCount(conId, cb){
    let select = 'select count(*) as cnt from runs r ';
    select += 'left join events e on r.event_id = e.id ';
    select += 'left join event_categories ec on e.event_category_id = ec.id ';
    select += 'where upper(ec.name) = \'LARP\' and e.status = \'active\' and e.convention_id = $1 ';
    select += exemptGames();

    database.query(select, [conId], function(err, result){
        if (err) { return cb(err); }
        cb(null, result.rows[0].cnt);
    });
}

function getCountedSlots (conId, cb){
    let output = {
        slots: 0,
        slotHours: 0,
        slotNPC:0,
        slotNPCHours: 0,
    };
    const eventSizes = [];
    const sizes = [];
    const sizesNPC = [];

    getEvents(conId, function(err, events){
        if (err) { return cb(err); }
        for (const event of events){
            const reg_policy = event.registration_policy;
            const buckets = reg_policy.buckets;
            let size = 0;
            let npcSize = 0;
            for(const bucket of buckets){
                console.log(event.title + ': ' + bucket.name + ': ' + bucket.not_counted + ': ' + bucket.total_slots + ': ' + event.runCount);
                if (bucket.not_counted !== true){
                    output.slots += (bucket.total_slots * event.data.runCount);
                    output.slotHours += (bucket.total_slots * event.data.runCount * (event.length_seconds / (60*60)));
                    size += bucket.total_slots;
                } else {
                    const unlim_signups = _.where(event.data.signups, {bucket_key: bucket.key, state:'confirmed'} );
                    if (unlim_signups){
                        output.slotNPC += unlim_signups.length;
                        output.slotNPCHours += unlim_signups.length * (event.length_seconds / (60*60));
                        npcSize += unlim_signups.length;
                    }
                }
            }
            for (let i = 0; i < event.data.runCount; i++){
                //if (size > 0 && size < 60){
                sizes.push(size);
                sizesNPC.push(size + npcSize);
                //}
            }
            console.log(event.title + ': ' + size);
            eventSizes.push(size);
        }
        output.runstats = statsArray(sizes);
        output.eventStats = statsArray(eventSizes);
        output.runStatsNPC = statsArray(sizesNPC);
        cb(null, output);
    });
}


function statsArray(arr){
    const output = {};
    const arraySorted = arr.sort(function(a, b){return Number(a)-Number(b);});
    output.size = arraySorted.length;
    output.sum = arraySorted.reduce((e,o) => {o+=e; return o;}, 0);
    output.avg = Math.round(output.sum / output.size *10) / 10;
    output.min = arraySorted[0];
    output.max = arraySorted[arraySorted.length-1];
    output.median = arraySorted[Math.round(arraySorted.length/2)];
    return output;
}

function getEvents(conId, cb){
    let select = 'select e.* from events e ';
    select += 'left join event_categories ec on e.event_category_id = ec.id ';
    select += 'where upper(ec.name) = \'LARP\' and e.status = \'active\' and e.convention_id = $1 ';
    select += exemptGames();

    database.query(select, [conId], function(err, result){
        if (err) { return cb(err); }
        const events = result.rows;
        async.map(events, function(event, cb){
            async.parallel({
                runCount: function(cb){
                    let runselect = 'select count(*) as cnt from runs where event_id = $1';
                    database.query(runselect, [event.id], function(err, runResult){
                        if (err) { return cb(err); }
                        cb (null, runResult.rows[0].cnt);
                    });
                },
                signups: function(cb){
                    let signupSelect = 'select * from signups left join runs on signups.run_id = runs.id where runs.event_id = $1';
                    database.query(signupSelect, [event.id], function(err, result){
                        if (err) { return cb(err); }
                        cb(null, result.rows);
                    });
                }
            }, function(err, result){
                if (err) { return cb(err); }
                event.data = result;
                cb(null, event);
            });
        }, cb);
    });
}

function getEarlySignups(conId, cb){
    const earlySignups = {};
    async.parallel({
        signups: function(cb){
            getLarpSignups(conId, cb);
        },
        schedule: function(cb){
            getSignupSchedule(conId, cb);
        }
    }, function(err, result){
        if (err) { return cb(err); }
        for (const signup of result.signups){
            if (signup.bucket.match(/^npc/i) && signup.state === 'confirmed'){
                const category = categorizeSignup(signup, result.schedule);
                if (!_.has(earlySignups, category)){
                    earlySignups[category] = 0;
                }
                earlySignups[category]++;
            }
        }
        cb(null, earlySignups);
    });
}

function categorizeSignup(signup, schedule){
    const signupTime = new Date(signup.created_at);
    for(const row of schedule){
        if (row.start){
            const scheduleTime = new Date(row.start);
            if (signupTime < scheduleTime){
                return 'Before ' + row.value;
            }
        }
    }
    return 'Unknown';
}

function getLarpSignups(conId, cb){
    let select = 'select s.created_at, s.updated_at, s.bucket_key, s.state, e.title, r.starts_at, u.email, e.registration_policy ';
    select += 'from signups s ';
    select += 'left join runs r on s.run_id = r.id ';
    select += 'left join events e on r.event_id = e.id ';
    select += 'left join event_categories ec on e.event_category_id = ec.id ';
    select += 'left join user_con_profiles ucp on ucp.id = s.user_con_profile_id ';
    select += 'left join users u on ucp.user_id = u.id ';
    select += 'where e.convention_id = $1 and ';
    select += 'upper(ec.name) = \'LARP\' and e.status = \'active\'';
    select += 'order by s.created_at asc';

    database.query(select, [conId], function(err, result){
        if (err) { return cb(err); }
        const signups = result.rows.map(signup => {
            if (signup.bucket_key){
                const reg_policy = signup.registration_policy;
                const buckets = reg_policy.buckets;
                const bucket = _.findWhere(buckets, {key: signup.bucket_key});
                if(bucket){
                    signup.bucket = bucket.name;
                } else {
                    signup.bucket = 'unknown';
                }
            } else {
                signup.bucket = 'Event Team Member';
            }
            return signup;
        });
        cb(null, signups);
    });
}

function getSignupSchedule(conId, cb){
    const select = 'select maximum_event_signups from conventions where id = $1';
    database.query(select, [conId], function(err, result){
        if (err) { return cb(err); }
        cb(null, result.rows[0].maximum_event_signups.timespans);
    });
}

function exemptGames(){
    const exemptions = [
        'Ops',
        'ConSuite',
        'Ops Track',
        'Ops!',
        'Intercon Sunday Breakfast',
        'NEIL Annual Board Meeting',
        'The Eclectic Dance Mix Party',
        'Friday Night Coffeehouse',
        'Toast to Brett',
        'Intercon: The Leaving',
        'Rising Phoenix Party',
        '[CANCELLED] Pendragon: Tales on a Winter\'\'s Morn',
        'A Toast to Dean Edgell',
    ];
    return ' and e.title not in ( \'' + exemptions.join('\', \'')+ '\')';
}
