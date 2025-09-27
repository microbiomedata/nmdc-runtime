console.debug("Listening for event: nmdcInit");
window.addEventListener("nmdcInit", (event) => {
    console.debug("Detected event: nmdcInit");

    // Get the DOM elements we'll be referencing below.
    const bodyEl = document.querySelector("body");

    // If there is a non-empty access token present in the DOM (see `main.py`), create and add a banner
    // displaying the token along with buttons to show/hide it and copy it to the clipboard.
    const accessToken = document.getElementById("nmdc-access-token")?.getAttribute("data-token");
    if (typeof accessToken === "string" && accessToken.trim().length > 0) {
        console.debug("Adding token banner");

        // Create the banner.
        const sectionEl = document.createElement("section");
        sectionEl.classList.add("nmdc-info", "nmdc-info-token", "block", "col-12");
        sectionEl.innerHTML = `
            <p>You are now authorized. Prefer a command-line interface (CLI)? Use this header for HTTP requests:</p>
            <p>
                <code>
                    <span>Authorization: Bearer </span>
                    <span id="token" data-state="masked">***</span>
                </code>
            </p>
            <p>
                <button id="token-mask-toggler">Show token</button>
                <button id="token-copier">Copy token</button>
                <span id="token-copier-message"></span>
            </p>
        `;

        // Mount the banner to the DOM.
        document.querySelector(".information-container").append(sectionEl);

        // Get references to DOM elements within the banner that was mounted to the DOM.
        const tokenMaskTogglerEl = document.getElementById("token-mask-toggler");
        const tokenEl = document.getElementById("token");
        const tokenCopierEl = document.getElementById("token-copier");
        const tokenCopierMessageEl = document.getElementById("token-copier-message");

        // Set up the token visibility toggler.
        console.debug("Setting up token visibility toggler");
        tokenMaskTogglerEl.addEventListener("click", (event) => {
            if (tokenEl.dataset.state == "masked") {
                console.debug("Unmasking token");
                tokenEl.dataset.state = "unmasked";
                tokenEl.textContent = accessToken;
                event.target.textContent = "Hide token";
            } else {
                console.debug("Masking token");
                tokenEl.dataset.state = "masked";
                tokenEl.textContent = "***";
                event.target.textContent = "Show token";
            }
        });

        // Set up the token copier.
        // Reference: https://developer.mozilla.org/en-US/docs/Web/API/Clipboard/writeText
        console.debug("Setting up token copier");
        tokenCopierEl.addEventListener("click", async (event) => {
            tokenCopierMessageEl.textContent = "";
            try {
                await navigator.clipboard.writeText(accessToken);
                tokenCopierMessageEl.innerHTML = "<span class='nmdc-success'>Copied to clipboard</span>";
            } catch (error) {
                console.error(error.message);
                tokenCopierMessageEl.innerHTML = "<span class='nmdc-error'>Copying failed</span>";
            }
        });
    }

    /**
     * Customizes the login form in the following ways:
     * - Changes the header text of the username/password login form to "User login".
     * - Changes the header text of the client credentials login form to "Site client login".
     * - Augments the "Logout" button on the `bearerAuth` login form so that, when it is clicked,
     *   it clears and expires the `user_id_token` cookie, and reloads the web page.
     * - Focuses on the username input field whenever the login form appears.
     * - Adds a "Login with ORCID" widget to the login form.
     * 
     * Reference: https://developer.mozilla.org/en-US/docs/Web/API/Document/cookie
     * 
     * Prerequisite: The login form must be present in the DOM.
     */
    const customizeLoginForm = () => {
        const modalContentEl = document.querySelector('.auth-wrapper .modal-ux-content');
        const formHeaderEls = modalContentEl.querySelectorAll('.auth-container h4');
        formHeaderEls.forEach(el => {
            switch (el.textContent.trim()) {
                case "OAuth2PasswordOrClientCredentialsBearer (OAuth2, password)":
                    console.debug(`Customizing "password" login form header`);
                    el.textContent = "User login";
                    break;
                case "OAuth2PasswordOrClientCredentialsBearer (OAuth2, clientCredentials)":
                    console.debug(`Customizing "clientCredentials" login form header`);
                    el.textContent = "Site client login";
                    break;
                // Note: This string has a `U+00a0` character before the regular space.
                case "bearerAuth  (http, Bearer)":
                    const buttonEls = el.closest(".auth-container").querySelectorAll("button");
                    buttonEls.forEach(buttonEl => {
                        if (buttonEl.textContent.trim() === "Logout") {
                            console.debug(`Augmenting "bearerAuth" form logout button`);
                            buttonEl.addEventListener("click", () => {
                                console.debug("Clearing and expiring `user_id_token` cookie");
                                document.cookie = "user_id_token=; max-age=0; path=/;";
                                // Reload the web page so that any in-memory authentication state is reset.
                                // Note: If we had full control over the Swagger UI code, we would just
                                //       manipulate that state directly instead of reloading the page.
                                console.debug("Reloading web page");
                                window.location.reload();
                            });
                        }
                    });
                    break;
                default:
                    console.debug(`Unrecognized header: ${el.textContent}`);
            }
        });
        // Add a "Login with ORCID" widget to the login form.
        //
        // TODO: Consider disabling this when the user is already logged in.
        //
        // TODO: Consider moving this up next to (or into) the regular "User login" form,
        //       once our system administrators have implemented a practical process for
        //       managing "allowances" of users whose usernames are ORCID IDs. Putting it
        //       at the bottom of the modal (I think) makes it less likely people will use it.
        //
        console.debug("Adding ORCID Login widget to login form");
        const orcidLoginUrl = document.getElementById("nmdc-orcid-login-url")?.getAttribute("data-url");
        const orcidLoginWidgetEl = document.createElement("div");
        orcidLoginWidgetEl.classList.add("auth-container", "nmdc-orcid-login");
        orcidLoginWidgetEl.innerHTML = `
            <h4>User login with ORCID</h4>
            <div class="nmdc-orcid-login-icon-link">
                <img src="/static/ORCID-iD_icon_vector.svg" height="16" width="16"/>
                <a href="${orcidLoginUrl}">Login with ORCID</a>
            </div>
        `;
        modalContentEl.appendChild(orcidLoginWidgetEl);

        console.debug("Focusing on username field if present");
        const usernameInputEl = modalContentEl.querySelector("input#oauth_username");
        if (usernameInputEl !== null) {
            usernameInputEl.focus();
        }
    };
    console.debug("Setting up event listener for customizing login form");
    //
    // Listen for a "click" event on the `body` element, check whether the element that was clicked
    // was the "Authorize" button (or one of its descendants), and if so, customize the modal login
    // form that will have been mounted to the DOM by the time the "click" event propagated to the
    // `body` element and our event handler was called.
    //
    // Note: We attach this event listener to the `body` element because that's the lowest-level
    //       element where we found that mounting it doesn't cause our event handler to run too early
    //       (i.e. doesn't cause it to run _before_ the event handlers that mount the modal login form
    //       to the DOM have run). Our event handler needs that form to be mounted so it can access
    //       its elements.
    //       
    //       If we were to attach it to a lower-level element (e.g. directly to the "Authorize" button),
    //       we would have to, for example, make its body a `setTimeout(fn, 0)` callback in order to
    //       defer its execution until all the event handlers for the "click" even have run.
    //
    bodyEl.addEventListener("click", (event) => {
        // Check whether the clicked element was the "Authorize" button or any of its descendants.
        if (event.target.closest(".auth-wrapper > .btn.authorize:not(.modal-btn)") !== null) {
            customizeLoginForm();
        }
    });

    // Set up the togglers for the tag details.
    //
    // Note: At the time of this writing, all of our tag descriptions begin with a
    //       single-paragraph summary of the tag. Some of the tag descriptions have
    //       additional paragraphs that provide more _details_ about the tag. In an
    //       attempt to keep the Swagger UI page "initially concise" (only showing
    //       more information when the user requests it), for the tag descriptions
    //       that have additional paragraphs, we add a toggler button that the user
    //       can press to toggle the visibility of the additional paragraphs.
    //
    console.debug("Setting up tag description details togglers");
    const tagSectionEls = bodyEl.querySelectorAll(".opblock-tag-section");
    Array.from(tagSectionEls).forEach(el => {
        // Check whether the description contains more than one element (i.e. paragraph).
        const descriptionEl = el.querySelector("h3 > small > .renderedMarkdown");
        if (descriptionEl.children.length > 1) {
            // Wrap the additional elements (i.e. paragraphs) in a hidable `<div>`.
            const detailsEl = document.createElement("div");
            detailsEl.classList.add("tag-description-details", "hidden");
            Array.from(descriptionEl.children).slice(1).forEach(el => {
                detailsEl.appendChild(el);
            });
            descriptionEl.replaceChildren(descriptionEl.firstChild, detailsEl);

            // Add a button that toggles the visibility of the tag details.
            // Note: We use SVG to draw ellipses on the button, rather than
            //       loading an image or icon.
            const toggleButtonEl = document.createElement("button");
            toggleButtonEl.classList.add("tag-description-details-toggler");
            toggleButtonEl.title = "Toggle details";
            toggleButtonEl.innerHTML = `
                <!-- Ellipses -->
                <svg width="12" height="8" viewBox="0 0 12 8">
                    <ellipse cx="2" cy="4" rx="1" ry="1" fill="currentColor" />
                    <ellipse cx="6" cy="4" rx="1" ry="1" fill="currentColor" />
                    <ellipse cx="10" cy="4" rx="1" ry="1" fill="currentColor" />
                </svg>
            `;
            descriptionEl.firstChild.appendChild(toggleButtonEl);
            toggleButtonEl.addEventListener("click", (event) => {
                detailsEl.classList.toggle("hidden");
                event.stopPropagation();  // avoid expanding the tag section, itself
            });
        }
    });

    // If the `<endpoint-search-widget>` custom HTML element is available, add it to the DOM.
    // Note: That custom HTML element gets defined within the `EndpointSearchWidget.js` script.
    // Docs: https://developer.mozilla.org/en-US/docs/Web/API/Web_components/Using_custom_elements#using_a_custom_element
    if (customElements.get("endpoint-search-widget")) {
        console.debug("Setting up endpoint search widget");
        const endpointSearchWidgetEl = document.createElement("endpoint-search-widget");
        bodyEl.querySelector(".scheme-container").after(endpointSearchWidgetEl); // put it below the "Authorize" section
    }
});
