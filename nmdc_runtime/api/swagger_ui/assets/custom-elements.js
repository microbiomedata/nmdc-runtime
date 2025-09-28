/**
 * This JavaScript file contains the implementations of various Web Components
 * (which get defined as "custom elements") designed for use with Swagger UI.
 * Reference: https://developer.mozilla.org/en-US/docs/Web/API/Web_components/Using_custom_elements
 *
 * Developer notes:
 * 
 * 1. Because we define our CSS and HTML within `String.raw` tagged templates [1],
 *    the popular "lit-html" extension [2] (if installed) for VS Code will apply
 *    CSS and HTML syntax highlighting to those strings.
 * 
 *    References:
 *    - [1] https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Template_literals
 *    - [2] https://marketplace.visualstudio.com/items?itemName=bierner.lit-html
 *
 *****************************************************************************/

// If FastAPI's Swagger UI JavaScript hasn't been executed yet, throw an error explaining the situation.
// Note: FastAPI runs the Swagger UI JavaScript in a way that creates a global variable named `ui`.
// Note: We do a `typeof` check because, if we did `ui === undefined` and `ui` weren't defined,
//       JavaScript would raise its own `ReferenceError` exception with its own message.
if (typeof ui === "undefined") {
    throw new Error("FastAPI's Swagger UI JavaScript has not been executed yet.");
}

/**
 * Endpoint search widget, implemented as a Web Component.
 * 
 * The widget provides a search input field, and a results panel that lists
 * the API endpoints whose URL paths match the search term. When the user
 * clicks on a search result, the browser navigates to the corresponding
 * endpoint in the Swagger UI page (via a full page refresh/"deep link").
 *
 * References:
 * - https://developer.mozilla.org/en-US/docs/Web/API/Web_components
 * - https://open-wc.org/codelabs/basics/web-components#2
 */
class EndpointSearchWidget extends HTMLElement {
    constructor() {
        super();

        // Initialize all instance properties.
        this.inputEl = null;
        this.resultsPanelEl = null;
        this.noEndpointsMessageEl = null;
        this.resultsListEl = null;
        this.endpoints = [];
    }

    /**
     * Initializes the custom HTML element after it's been added to the DOM.
     * 
     * Note: The `connectedCallback` function gets called automatically when
     *       this HTML element gets added to the DOM.
     */
    connectedCallback() {
        // Create a Shadow DOM tree specific to this custom HTML element, so that its styles don't
        // impact other elements on the page and so styles on the page don't impact this element. [1]
        //
        // Note: This will populate the instance's inherited `this.shadowRoot` property [2]
        //       with a reference to the root node of this Shadow DOM tree. [3]
        //
        // References: 
        // - [1] https://developer.mozilla.org/en-US/docs/Web/API/Web_components/Using_shadow_DOM#creating_a_shadow_dom
        // - [2] https://developer.mozilla.org/en-US/docs/Web/API/Element/shadowRoot
        // - [3] https://developer.mozilla.org/en-US/docs/Web/API/ShadowRoot
        //
        this.attachShadow({ mode: "open" });

        // Implement the structure and style of the widget.
        //
        // Note: We chose the specific style rules shown below in an attempt to
        //       match the "look and feel" of the Swagger UI page. At least some
        //       of the seemingly-arbitrarily-chosen values were copied verbatim
        //       from native Swagger UI elements (via a web browser's DevTools).
        //
        this.shadowRoot.innerHTML = String.raw`
            <style>
                :root {
                    margin: 0 auto;
                    max-width: 1460px;
                    font-family: sans-serif;
                    font-size: 14px;
                    color: #3b4151;
                }
                .container {
                    margin: 20px 20px 0px 20px;
                }
                input {
                    padding: 8px 10px;
                    box-sizing: border-box;
                    border: 1px solid #d9d9d9;
                    border-radius: 4px;
                    width: 100%;
                }
                .results-panel {
                    background-color: rgba(0, 0, 0, .05);
                    border-radius: 4px;
                }
                .results-panel p.no-endpoints {
                    padding: 26px;
                }
                ul {
                    list-style-type: none;
                    padding-left: 26px;
                    padding-right: 26px;
                    font-family: monospace;
                }
                ul > li:first-child {
                    padding-top: 26px;
                }
                ul > li:last-child {
                    padding-bottom: 26px;
                }
                li > a {
                    color: #4990e2;
                    text-decoration: none;
                }
                li > a:hover {
                    color: #1f69c0;
                }
                li > a .http-method {
                    display: inline-block;
                    min-width: 52px;
                    margin-right: 10px;
                }
                .matching-substring {
                    background-color: #f9f871;
                }
                .hidden {
                    /* 
                    Note: When we hide elements via 'display: none', we do not have to also set 'aria-hidden="true"'.
                    MDN says: 'aria-hidden="true" should not be added when [...] The element [...] is hidden with 'display: none'.
                    Reference: https://developer.mozilla.org/en-US/docs/Web/Accessibility/ARIA/Reference/Attributes/aria-hidden
                    */
                    display: none;
                }
            </style>

            <div class="container">
                <input name="search-term" placeholder="Find an endpoint..." aria-label="Search term for finding an endpoint" />
                <div class="results-panel">
                    <p class="no-endpoints hidden">No matching endpoints.</p>
                    <ul class="results-list"></ul>
                </div>
            </div>
        `;
        this.inputEl = this.shadowRoot.querySelector("input");
        this.resultsPanelEl = this.shadowRoot.querySelector(".results-panel");
        this.noEndpointsMessageEl = this.shadowRoot.querySelector(".no-endpoints");
        this.resultsListEl = this.shadowRoot.querySelector(".results-list");

        // Make an array of all the endpoints that Swagger UI knows about. This will be our search index.
        //
        // Note: In an earlier implementation of this step, we queried the DOM for this information.
        //       However, that didn't work when any of the endpoint groups were collapsed. So, instead,
        //       we access the data structure returned by the `SwaggerUI()` constructor, which is
        //       invoked by the JavaScript code built into FastAPI (that JavaScript code assigns
        //       the return value to a global variable named `ui`, which we access here).
        //
        // Note: Should we eventually choose to display endpoint's description also, we can be obtain it
        //       via the endpoint's operation Map; i.e. `opMap.get("operation").get("description")`.
        //
        // Reference: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Map/get
        //
        const operationMaps = Array.from(ui.specSelectors.operations());
        this.endpoints = operationMaps.map(opMap => ({
            urlPath: opMap.get("path"), // e.g. "/studies/{study_id}"
            httpMethod: opMap.get("method").toUpperCase(), // e.g. "GET"
            operationId: opMap.get("operation").get("operationId"), // e.g. "find_studies_studies_get"
            tag: opMap.get("operation").get("tags").get(0), // e.g. "Metadata access: Find"
        }));
        console.debug(`Found ${this.endpoints.length} endpoints in OpenAPI schema`);

        // Make it so the search results list gets updated whenever the user changes the search input.
        // Reference: https://developer.mozilla.org/en-US/docs/Web/API/Element/input_event
        this.inputEl.addEventListener("input", (event) => {
            const inputValue = event.target.value;
            this.updateSearchResults(inputValue);
        });
    }

    /**
     * Callback function designed to be passed in as the `compareFn` argument to `Array.prototype.sort()`.
     * This callback function that sorts endpoint objects by comparing the URL paths and, if those are
     * equal, comparing the HTTP methods.
     * 
     * @param {{ urlPath: string, httpMethod: string }} a 
     * @param {{ urlPath: string, httpMethod: string }} b 
     * @returns {number} A negative number if `a` comes before `b`,
     *                   a positive number if `a` comes after `b`,
     *                   and 0 if `a` and `b` are equivalent.
     * 
     * References:
     * - https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/sort#comparefn
     */
    _sortEndpoints(a, b) {
        if (a.urlPath.localeCompare(b.urlPath) === 0) {
            return a.httpMethod.localeCompare(b.httpMethod);
        } else {
            return a.urlPath.localeCompare(b.urlPath);
        }
    }

    /**
     * Updates the search results list so it shows the endpoints whose URL paths contain the search term.
     * 
     * @param {string} searchTerm The search term.
     */
    updateSearchResults(searchTerm) {
        // Special case: If the search term is empty, clear the search results.
        if (searchTerm.trim().length === 0) {
            this.resultsListEl.replaceChildren();
            this.noEndpointsMessageEl.classList.add("hidden");
            return;
        }

        // Lowercase the search term to enable case-insensitive comparison.
        const lowercaseSearchTerm = searchTerm.toLowerCase();
        
        // Make a list of all endpoints whose URL paths contains the search term.
        const matchingEndpoints = this.endpoints.filter(item => {
            const lowercaseUrlPath = item.urlPath.toLowerCase();
            return lowercaseUrlPath.includes(lowercaseSearchTerm);
        });
        
        // If there are no matching endpoints, clear the search results.
        if (matchingEndpoints.length === 0) {
            this.resultsListEl.replaceChildren();
            this.noEndpointsMessageEl.classList.remove("hidden");
            return;
        } else {
            this.noEndpointsMessageEl.classList.add("hidden");
        }

        // Build a deep link for each matching endpoint (sorted by URL path, then HTTP method).
        // Reference: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/localeCompare
        const resultEls = matchingEndpoints.sort(this._sortEndpoints).map(matchingEndpoint => {
            const liEl = document.createElement("li");
            const aEl = document.createElement("a");

            // Wrap the part of the URL path that matches the search term, so that we can
            // style it differently from the rest of the URL path.
            const urlPathSpanEl = document.createElement("span");
            const lowercaseUrlPath = matchingEndpoint.urlPath.toLowerCase();
            const substrCharIdx = lowercaseUrlPath.indexOf(lowercaseSearchTerm);
            const substrSpanEl = document.createElement("span");
            const preSubstrChars = matchingEndpoint.urlPath.substring(0, substrCharIdx);
            const substrChars = matchingEndpoint.urlPath.substring(substrCharIdx, substrCharIdx + searchTerm.length);
            const postSubstrChars = matchingEndpoint.urlPath.substring(substrCharIdx + searchTerm.length);
            substrSpanEl.classList.add("matching-substring");
            substrSpanEl.textContent = substrChars;
            urlPathSpanEl.append(preSubstrChars, substrSpanEl, postSubstrChars);

            // Build a Swagger UI-compliant "deep link" to the corresponding endpoint,
            // using the syntax shown in the Swagger UI "Deep Linking" documentation, at:
            // https://swagger.io/docs/open-source-tools/swagger-ui/usage/deep-linking/#usage
            //
            // Note: The reason we don't use something like `scrollTo` or `scrollIntoView`
            //       is that the target element may not be mounted to the DOM right now,
            //       due to it being within a collapsed section. Instead, we send the
            //       browser to the "deep link" URL (Swagger UI will automatically expand
            //       the relevant section when the page loads that "deep link" URL).
            //
            const urlWithoutQueryStr = window.location.origin + window.location.pathname;
            const tagPart = encodeURIComponent(matchingEndpoint.tag);
            const operationPart = encodeURIComponent(matchingEndpoint.operationId);
            const httpMethodSpanEl = document.createElement("span");
            httpMethodSpanEl.classList.add("http-method");  // e.g. "<span class="http-method">GET</span>"
            httpMethodSpanEl.textContent = matchingEndpoint.httpMethod;
            aEl.append(httpMethodSpanEl, urlPathSpanEl);
            aEl.href = `${urlWithoutQueryStr}/#${tagPart}/${operationPart}`;
            liEl.appendChild(aEl);
            return liEl;
        });
        this.resultsListEl.replaceChildren(...resultEls);
    }
}

/**
 * Ellipses button with tooltip, implemented as a Web Component.
 * 
 * The tooltip has a slot [1], which can be used to specify the tooltip's text.
 * 
 * References:
 * - [1] https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/slot
 */
class EllipsesButton extends HTMLElement {
    constructor() {
        super();
    }

    connectedCallback() {
        this.attachShadow({ mode: "open" });
        this.shadowRoot.innerHTML = String.raw`
            <style>
                .container {
                    display: inline-flex;
                }
                button {
                    cursor: pointer;
                    background-color: transparent;
                    border: none;
                }
                button:hover {
                    color: #1f69c0;
                }

                .tooltip-wrapper {
                    cursor: auto;
                    display: inline-flex;
                    align-items: center;
                    opacity: 0;
                    transition: opacity 0.2s ease-in-out;
                }
                button:hover + .tooltip-wrapper {
                    opacity: 1;
                }
                .tooltip-arrow {
                    margin-right: -4px;
                    fill: #f4f4f4;
                }
                .tooltip-box {
                    font-family: sans-serif;
                    background: #f4f4f4;
                    color: #333;
                    padding: 4px 8px;
                    border-radius: 4px;
                    font-size: 11px;
                }
            </style>

            <span class="container">
                <button aria-describedby="tooltip" name="ellipses-button">
                    <!-- Ellipses (row of three dots) -->
                    <svg width="12" height="8" viewBox="0 0 12 8">
                        <ellipse cx="2" cy="4" rx="1" ry="1" fill="currentColor" />
                        <ellipse cx="6" cy="4" rx="1" ry="1" fill="currentColor" />
                        <ellipse cx="10" cy="4" rx="1" ry="1" fill="currentColor" />
                    </svg>
                </button>

                <!-- Tooltip (rectangle with left-pointing arrowhead) -->
                <div class="tooltip-wrapper">
                    <svg class="tooltip-arrow" width="16" height="24" viewBox="0 0 16 24">
                        <polygon points="0,12 16,6 16,18" />
                    </svg>
                    <div class="tooltip-box">
                        <slot role="tooltip">...</slot>
                    </div>
                </div>
            </span>
        `;

        // Prevent the propagation of click events occurring within the tooltip.
        // Note: This way, clicking the tooltip doesn't trigger any click handlers
        //       attached to the container.
        this.shadowRoot.querySelector(".tooltip-wrapper").addEventListener("click", (event) => {
            event.stopPropagation();
        });
    }
}

// Register custom HTML elements (i.e. `<endpoint-search-widget>` and `<ellipses-button>`).
// Docs: https://developer.mozilla.org/en-US/docs/Web/API/Web_components/Using_custom_elements#registering_a_custom_element
customElements.define("endpoint-search-widget", EndpointSearchWidget);
customElements.define("ellipses-button", EllipsesButton);
