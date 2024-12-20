// Example: Hit Ratio Chart using Chart.js
const ctx = document.getElementById('errorChart').getContext('2d');
fetch('/api/metrics')
  .then(response => response.json())
  .then(data => {
    const chart = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['Processed Hits', 'Remaining'],
        datasets: [{
          data: [data.hit_ratio, 100 - data.hit_ratio],
          backgroundColor: ['#36A2EB', '#FF6384']
        }]
      }
    });
  });
