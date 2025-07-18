
// --- Date and Time ---
function pad(n) { return n < 10 ? '0' + n : n; }
function getPSTDate() {
  const now = new Date();
  let utc = now.getTime() + (now.getTimezoneOffset() * 60000);
  let year = now.getFullYear();
  let dstStart = new Date(Date.UTC(year, 2, 8, 10, 0, 0));
  dstStart.setDate(8 + (7 - dstStart.getUTCDay()) % 7);
  let dstEnd = new Date(Date.UTC(year, 10, 1, 9, 0, 0));
  dstEnd.setDate(1 + (7 - dstEnd.getUTCDay()) % 7);
  let isDST = (now >= dstStart && now < dstEnd);
  let offset = isDST ? -7 : -8;
  return new Date(utc + 3600000 * offset);
}
function updateDateTime() {
  const pst = getPSTDate();
  const days = ["SUNDAY","MONDAY","TUESDAY","WEDNESDAY","THURSDAY","FRIDAY","SATURDAY"];
  const months = ["JANUARY","FEBRUARY","MARCH","APRIL","MAY","JUNE","JULY","AUGUST","SEPTEMBER","OCTOBER","NOVEMBER","DECEMBER"];
  const dateStr = `${months[pst.getMonth()]} ${pst.getDate()}, ${pst.getFullYear()} - ${days[pst.getDay()]}`;
  document.getElementById('date-str').textContent = dateStr;
  
  /*
  let h = pst.getHours(), m = pst.getMinutes();
  let ampm = h >= 12 ? "PM" : "AM";
  let hour12 = h % 12; if (hour12 === 0) hour12 = 12;
  let min = pad(m);
  document.getElementById('time-str').textContent = `${hour12}:${min} ${ampm}`;
  */
}
setInterval(updateDateTime, 1000);
updateDateTime();

// --- Slideshow ---
const images = document.querySelectorAll('.slide-img');
let idx = 0;
setInterval(() => {
  images[idx].classList.remove('active');
  idx = (idx + 1) % images.length;
  images[idx].classList.add('active');
}, 2000);




  // --- Subscribe Form ---
  document.getElementById('subscribe-form').addEventListener('submit', async function(e) {
  e.preventDefault();
  const email = document.getElementById('subscribe-email').value.trim();
  const msgDiv = document.getElementById('subscribe-message');
  msgDiv.textContent = '';
  if (!email) {
    msgDiv.textContent = 'Please enter your email.';
    msgDiv.style.color = '#ff1616';
    return;
  }
  try {
    const resp = await fetch('/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email })
    });
    if (resp.ok) {
      msgDiv.textContent = 'Thank you for subscribing!';
      msgDiv.style.color = '#39FF14';
      document.getElementById('subscribe-form').reset();
    } else {
      const data = await resp.json().catch(() => ({}));
      msgDiv.textContent = data.error || 'Subscription failed. Please try again.';
      msgDiv.style.color = '#ff1616';
    }
  } catch (err) {
    msgDiv.textContent = 'Could not connect. Please try again later.';
    msgDiv.style.color = '#ff1616';
  }
});
