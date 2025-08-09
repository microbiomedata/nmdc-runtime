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
            tokenEl.textContent = tokenEl.dataset.tokenValue;
            event.target.textContent = "Hide token";
        }} else {{
            console.debug("Masking token");
            tokenEl.dataset.state = "masked";
            tokenEl.textContent = "***";
            event.target.textContent = "Show token";
        }}
    }});

    // Set up the token copier.
    // Reference: https://developer.mozilla.org/en-US/docs/Web/API/Clipboard/writeText
    console.debug("Setting up token copier");
    tokenCopierEl.addEventListener("click", async (event) => {{
        tokenCopierMessageEl.textContent = "";
        try {{                            
            await navigator.clipboard.writeText(tokenEl.dataset.tokenValue);
            tokenCopierMessageEl.innerHTML = "<span class='nmdc-success'>Copied to clipboard</span>";
        }} catch (error) {{
            console.error(error.message);
            tokenCopierMessageEl.innerHTML = "<span class='nmdc-error'>Copying failed</span>";
        }}
    }})

    // Simplify authentication field header text
    console.debug("Simplifying authentication field header text");
    // Wait a short moment for Swagger UI to fully render
    setTimeout(() => {{
        // Look for the authorization button or text that contains the technical OAuth2 name
        const authElements = document.querySelectorAll('button, span, div');
        authElements.forEach(element => {{
            if (element.textContent && element.textContent.includes('OAuth2PasswordOrClientCredentialsBearer')) {{
                console.debug("Found OAuth2 authentication element, replacing text");
                element.textContent = element.textContent.replace('OAuth2PasswordOrClientCredentialsBearer (OAuth2, password)', 'User Login');
                element.textContent = element.textContent.replace('OAuth2PasswordOrClientCredentialsBearer', 'User Login');
            }}
        }});
    }}, 500);
}});
