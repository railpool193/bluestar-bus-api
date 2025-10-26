function two(n){return (n<10?'0':'')+n;}
function tickClock(){
  const d = new Date();
  const t = `${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())}`;
  const el = document.getElementById('ukClock');
  if(el) el.textContent = `UK: ${t}`;
}
tickClock(); setInterval(tickClock,1000);
