function pad(n){return n.toString().padStart(2,'0')}
function fmtCountdown(ms){
  if(ms<=0) return "0:00";
  const s=Math.floor(ms/1000), m=Math.floor(s/60), r=s%60;
  return `${m}:${pad(r)}`
}
function tickCountdown(){
  const now=Date.now();
  document.querySelectorAll("[data-eta]").forEach(el=>{
    const t=parseInt(el.dataset.eta,10);
    const left=t-now;
    el.textContent=fmtCountdown(left);
    if(left<=0){ el.classList.add("over"); }
  });
}
setInterval(tickCountdown,1000); tickCountdown();

// UK óra frissítése
function tickClock(){
  const el=document.querySelector("#ukclock");
  if(!el) return;
  const now=new Date();
  el.textContent = now.toLocaleTimeString("en-GB",{hour12:false});
}
setInterval(tickClock,1000); tickClock();
