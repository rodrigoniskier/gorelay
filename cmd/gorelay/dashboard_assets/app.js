'use strict';
(() => {
 const $ = id => document.getElementById(id);
 let overviewBusy = false, demoBusy = false, demo = {enabled:false}, timer;
 let detailRequest, detailInvokerID;
 const number = value => typeof value === 'number' && Number.isFinite(value) ? value.toLocaleString() : '—';
 const date = value => { if (!value) return '—'; const d = new Date(value); return Number.isNaN(d.getTime()) ? '—' : d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'}); };
 const statuses = {pending:'Pending',processing:'Processing',retry_wait:'Retry scheduled',delivered:'Delivered',dead_letter:'Dead-letter',processed:'Processed (legacy)'};
 async function api(path, options = {}) {
  const controller = new AbortController();
  const cancel = () => controller.abort();
  if(options.signal?.aborted) controller.abort();
  options.signal?.addEventListener('abort', cancel, {once:true});
  const timeout = setTimeout(cancel, 10000);
  try {
   const response = await fetch(path,{...options,cache:'no-store',credentials:'same-origin',signal:controller.signal});
   const data = await response.json();
   if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
   return data;
  } finally {clearTimeout(timeout);options.signal?.removeEventListener('abort',cancel);}
 }
 function cell(row, text, className) { const node = document.createElement('td'); node.textContent = text; if(className) node.className=className; row.append(node); return node; }
 function badge(status) {const span=document.createElement('span');span.className='badge '+(Object.hasOwn(statuses,status)?status:'');span.textContent=statuses[status]||'Unknown';return span;}
 function focusEvent(id) {
  const action=Array.from($('event-rows').querySelectorAll('.detail-button')).find(button=>button.dataset.eventId===id);
  (action || $('events-title')).focus();
 }
 function renderEvents(events) {
  const focusedEventID=document.activeElement?.dataset.eventId;
  $('event-rows').replaceChildren();
  $('event-count-label').textContent=number(events.length);
  if (!events.length) {const row=document.createElement('tr');const td=cell(row,'No persisted events yet. Enable the local demo to generate a batch.','empty');td.colSpan=7;$('event-rows').append(row);if(focusedEventID)focusEvent(focusedEventID);return;}
  for(const event of events) {
   const row=document.createElement('tr');
   const id=cell(row,event.id.slice(0,8)+'…','mono');id.title=event.id;
   const type=cell(row,event.type);const destination=document.createElement('span');destination.className='destination';destination.textContent=event.destination_id || 'No destination (legacy)';type.append(destination);
   const status=cell(row,'');status.append(badge(event.status));
   cell(row,number(event.attempts));cell(row,date(event.next_attempt_at));cell(row,date(event.created_at));
   const action=cell(row,'');const button=document.createElement('button');button.className='detail-button';button.dataset.eventId=event.id;button.textContent='Inspect ↗';button.setAttribute('aria-label','Inspect event '+event.id);button.addEventListener('click',()=>showDetails(event.id));action.append(button);
   $('event-rows').append(row);
  }
  if(focusedEventID)focusEvent(focusedEventID);
 }
 async function refresh() {
  if(overviewBusy) return;
  overviewBusy=true;$('metrics').setAttribute('aria-busy','true');$('refresh').disabled=true;
  try {
   const data=await api('/dashboard/api/overview');
   for(const [id,key] of Object.entries({'metric-total':'total','metric-delivered':'delivered','pipeline-delivered':'delivered','metric-retry':'retry_wait','metric-dead-letter':'dead_letter','metric-pending':'pending','metric-processing':'processing','active-leases':'active_leases'})) $(id).textContent=number(data[key]);
   $('success-rate').textContent=data.success_rate == null ? '—' : data.success_rate.toFixed(1)+'%';
   $('success-denominator').textContent=`Delivered / (delivered + dead-letter): ${number(data.success_numerator)} / ${number(data.success_denominator)}. Legacy processed events excluded.`;
   $('latency').textContent=data.successful_processing_ms == null ? '—' : data.successful_processing_ms.toFixed(1)+' ms';
   renderEvents(data.events);$('error').hidden=true;
   $('live-label').textContent='Live · 3s refresh';$('live-dot').className='local-dot';
   $('updated').textContent='Updated '+new Date().toLocaleTimeString();
  } catch(error) {
   $('error').textContent='Unable to refresh persisted data. '+error.message+' Previous values may be stale.';$('error').hidden=false;$('live-label').textContent='Connection interrupted';$('updated').textContent='Refresh failed · data may be stale';
  } finally {overviewBusy=false;$('metrics').setAttribute('aria-busy','false');$('refresh').disabled=false;}
 }
 function demoControls() {
  const disabled=!demo.enabled||demoBusy;
  for(const button of document.querySelectorAll('[data-mode]')){button.disabled=disabled;button.setAttribute('aria-pressed',String(button.dataset.mode===demo.mode));}
  $('event-count').disabled=disabled;$('generate').disabled=disabled;
  $('demo-state').textContent=demo.enabled ? 'DEMO ENABLED · LOCAL RECIPIENT' : 'DEMO DISABLED · READ-ONLY';
 }
 async function loadDemo() {
  try{demo=await api('/dashboard/api/demo');demoControls();}
  catch(error){demo={enabled:false};demoControls();$('demo-feedback').textContent='Demo controls unavailable. '+error.message;}
 }
 async function control(path,body) {
  if(!demo.enabled || demoBusy) return;
  demoBusy=true;demoControls();$('demo-feedback').textContent='Applying local demo action…';
  try{const result=await api(path,{method:'POST',headers:{'Content-Type':'application/json','X-GoRelay-CSRF':demo.csrf_token},body:JSON.stringify(body)});return result;}
  catch(error){$('demo-feedback').textContent=error.message;throw error;}
  finally{demoBusy=false;demoControls();}
 }
 for(const button of document.querySelectorAll('[data-mode]')) button.addEventListener('click', async()=>{
  try{await control('/dashboard/api/demo/mode',{mode:button.dataset.mode});demo.mode=button.dataset.mode;demoControls();$('demo-feedback').textContent='Local recipient set to '+(demo.mode==='timeout'?'timeout':'HTTP '+demo.mode)+'. Existing dead-letter events remain retained.';}catch(_error){}
 });
 $('generate').addEventListener('click',async()=>{
  const count=Number($('event-count').value);
  if(!Number.isInteger(count)||count<1||count>100){$('demo-feedback').textContent='Choose a whole number from 1 to 100.';$('event-count').focus();return;}
  try{await control('/dashboard/api/demo/events',{count});$('demo-feedback').textContent=`Generated ${count} synthetic events. Observe the persisted outcomes below.`;await refresh();}catch(_error){}
 });
 async function showDetails(id) {
  detailInvokerID=id;
  if(detailRequest) detailRequest.abort();detailRequest=new AbortController();
  $('detail-body').textContent='Loading persisted attempt history…';$('event-dialog').showModal();
  try {
   const data=await api('/dashboard/api/events/'+encodeURIComponent(id),{signal:detailRequest.signal});
   const fragment=document.createDocumentFragment(), dl=document.createElement('dl');
   for(const [label,value] of [['Event ID',data.event.id],['Type',data.event.type],['Destination',data.event.destination_id || 'No destination (legacy)'],['Status',statuses[data.event.status] || data.event.status],['Consumed attempts',number(data.event.attempts)],['Created',date(data.event.created_at)],['Next attempt',date(data.event.next_attempt_at)]]){const dt=document.createElement('dt'),dd=document.createElement('dd');dt.textContent=label;dd.textContent=value;dl.append(dt,dd);}
   fragment.append(dl);const title=document.createElement('h3');title.textContent='Attempt history · latest 100';fragment.append(title);
   if(!data.attempts.length){const p=document.createElement('p');p.textContent='No recorded attempts. Legacy history is not reconstructed.';p.className='small-note';fragment.append(p);}
   else {
    const wrap=document.createElement('div');wrap.className='table-scroll';const table=document.createElement('table'),head=document.createElement('thead'),tr=document.createElement('tr');
    for(const label of ['#','Started','Outcome / failure','HTTP','Finished']){const th=document.createElement('th');th.textContent=label;tr.append(th);}head.append(tr);table.append(head);
    const body=document.createElement('tbody');for(const attempt of data.attempts){const row=document.createElement('tr');cell(row,number(attempt.number));cell(row,date(attempt.started_at));const result=cell(row,attempt.outcome || 'In progress');if(attempt.failure_code){const failure=document.createElement('span');failure.className='destination';failure.textContent=attempt.failure_code;result.append(failure);}cell(row,attempt.http_status == null ? '—' : String(attempt.http_status));cell(row,date(attempt.finished_at));body.append(row);}table.append(body);wrap.append(table);fragment.append(wrap);
   }
   $('detail-body').replaceChildren(fragment);
  }catch(error){if(error.name!=='AbortError') $('detail-body').textContent='Could not load event details. '+error.message;}
 }
 $('close-detail').addEventListener('click',()=>$('event-dialog').close());
 $('event-dialog').addEventListener('close',()=>{if(detailRequest)detailRequest.abort();if(detailInvokerID)focusEvent(detailInvokerID);detailInvokerID=undefined;});
 $('refresh').addEventListener('click',refresh);
 async function poll(){clearTimeout(timer);if(!document.hidden) await refresh();timer=setTimeout(poll,3000);}
 document.addEventListener('visibilitychange',()=>{if(!document.hidden)poll();});
 loadDemo();poll();
})();
