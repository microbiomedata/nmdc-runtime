console.debug("Listening for event: nmdcInit");
window.addEventListener("nmdcInit", (event) => {{
    // Get the DOM elements we'll be referencing below. 
    const tokenMaskTogglerEl = document.getElementById("token-mask-toggler");
    const tokenEl = document.getElementById("token");
    const tokenCopierEl = document.getElementById("token-copier");
    const tokenCopierMessageEl = document.getElementById("token-copier-message");
    
    // Set up the token visibility toggler.
    console.debug("Setting up token visibility toggler");
    tokenMaskTogglerEl.addEventListener("click", (event) => {{
        if (tokenEl.dataset.state == "masked") {{
            console.debug("Unmasking token");
            tokenEl.dataset.state = "unmasked";
            tokenEl.innerHTML = tokenEl.dataset.tokenValue;
            event.target.innerHTML = "Hide token";
        }} else {{
            console.debug("Masking token");
            tokenEl.dataset.state = "masked";
            tokenEl.innerHTML = "***";
            event.target.innerHTML = "Show token";
        }}
    }});

    // Set up the token copier.
    // Reference: https://developer.mozilla.org/en-US/docs/Web/API/Clipboard/writeText
    console.debug("Setting up token copier");
    tokenCopierEl.addEventListener("click", async (event) => {{
        tokenCopierMessageEl.innerHTML = "";
        try {{                            
            await navigator.clipboard.writeText(tokenEl.dataset.tokenValue);
            tokenCopierMessageEl.innerHTML = "<span class='nmdc-success'>Copied to clipboard</span>";
        }} catch (error) {{
            console.error(error.message);
            tokenCopierMessageEl.innerHTML = "<span class='nmdc-error'>Copying failed</span>";
        }}
    }})
}});
