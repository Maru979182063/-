(function () {
  function ensureCompatStyle() {
    if (document.getElementById("demoV2CompatStyle")) return;
    const style = document.createElement("style");
    style.id = "demoV2CompatStyle";
    style.textContent = `
      .inline-feedback strong {
        font-weight: 700;
      }
    `;
    document.head.appendChild(style);
  }

  document.addEventListener("DOMContentLoaded", () => {
    ensureCompatStyle();
    document.documentElement.dataset.demoV2ZhPatch = "20260408a";
  });
})();
