console.debug("Listening for event: nmdcInit");
window.addEventListener("nmdcInit", (event) => {{
    // Get the DOM elements we'll be referencing below. 
    const tokenMaskTogglerEl = document.getElementById("token-mask-toggler");
    const tokenEl = document.getElementById("token");
    const tokenCopierEl = document.getElementById("token-copier");
    const tokenCopierMessageEl = document.getElementById("token-copier-message");
    const authBtnEl = document.querySelector('.auth-wrapper > .btn.authorize');
    
    // If all the token visibility-related elements are present (according to the logic implemented in `main.py`,
    // they will be present if an access token is available), set up the token visibility toggler and token copier.
    if (tokenMaskTogglerEl !== null && tokenEl !== null && tokenCopierEl !== null && tokenCopierMessageEl !== null) {
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
        }});
    }

    // Customize the headers in the modal login form so they are more user-friendly.
    console.debug("Customizing login form headers");
    authBtnEl.addEventListener("click", (event) => {
        // Note: We wrap this in a `setTimeout` so that it runs after all other event handlers
        //       listening for the "click" event have finished running. One of those event
        //       handlers mounts the login form to the DOM, which is a prerequisite of us
        //       accessing its elements below. We use a timeout of 0 milliseconds to
        //       effectively schedule this to run "as soon as possible" after the
        //       original "click" event has been handled.
        setTimeout(() => {
            const modalContentEl = document.querySelector('.auth-wrapper .modal-ux-content');
            const formHeaderEls = modalContentEl.querySelectorAll('.auth-container h4');
            formHeaderEls.forEach(el => {
                // Update the header text based on its current value.
                switch (el.textContent) {
                    // Note: This default string has a trailing space.
                    case "OAuth2PasswordOrClientCredentialsBearer (OAuth2, password) ":
                        el.textContent = "User login";
                        break;
                    // Note: This default string has a trailing space.
                    case "OAuth2PasswordOrClientCredentialsBearer (OAuth2, clientCredentials) ":
                        el.textContent = "Site client login";
                        break;
                    // Note: This default string has a `U+00a0` character before the space.
                    case "bearerAuth  (http, Bearer)":
                        break; // do nothing
                    default:
                        console.debug(`Unrecognized header: ${el.textContent}`);
                }
            });
        }, 0);
    });
}});
