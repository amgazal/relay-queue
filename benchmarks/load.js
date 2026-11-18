import http from 'k6/http';
import exec from 'k6/execution';

const count = Number(__ENV.COUNT);
const rate = Number(__ENV.RATE);
const scenario = rate
  ? {executor:'constant-arrival-rate',rate,timeUnit:'1s',duration:__ENV.SECONDS+'s',preAllocatedVUs:16,maxVUs:64}
  : {executor:'shared-iterations',vus:16,iterations:count,maxDuration:'4m'};
export const options = {scenarios:{submit:scenario}, thresholds:{http_req_failed:['rate==0']}};

export default function () {
  const i = exec.scenario.iterationInTest;
  if (i >= count) return;
  const key = __ENV.PREFIX + '-' + i;
  const payload = __ENV.KIND === 'webhook.deliver' ? {
    destination:'demo',
    event_id:(__ENV.PREFIX.endsWith('warm')?'00000000':__ENV.PREFIX.slice(1,9))+'-'+__ENV.PREFIX.slice(9,13)+'-4000-8000-'+i.toString(16).padStart(12,'0'),
    event_type:'benchmark.completed', data:{sequence:i},
  } : {};
  const body = JSON.stringify({type:__ENV.KIND,payload,idempotency_key:key});
  const opts = {headers:{Authorization:'Bearer '+__ENV.RELAY_API_KEY,'Content-Type':'application/json'},timeout:'8s'};
  function post() {
    const res = http.post('http://api:8080/v1/jobs',body,opts);
    const id = res.status===201 || res.status===200 ? res.json('id') : '';
    console.log(JSON.stringify([i,res.status,id,res.timings.duration,Date.now()]));
  }
  post();
  if (i%10===0) post();
}
