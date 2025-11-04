/**
 * Ellipses button with tooltip, implemented as a Web Component.
 * 
 * The tooltip has a slot [1], which can be used to specify the tooltip's text.
 * 
 * References:
 * - [1] https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/slot
 */
class EllipsesButton extends HTMLElement {
    // List the names of HTML attributes this element will respond to.
    // Reference: https://developer.mozilla.org/en-US/docs/Web/API/Web_components/Using_custom_elements#responding_to_attribute_changes
    static observedAttributes = ["is-open"];

    constructor() {
        super();

        this.tooltipWrapperEl = null;

        // Ensure that, within our callback methods, the "this" variable refers to this class instance.
        this.stopEventPropagation = this.stopEventPropagation.bind(this);
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
                    line-height: 0px;
                    border-radius: 9999px; /* circle */

                    transition: background-color 0.2s ease-in-out,
                                color 0.2s ease-in-out;
                }
                button:hover {
                    color: #1f69c0;
                    background-color: #f4f4f4;
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
                .hidden {
                    display: none;
                }
            </style>

            <span class="container">
                <button aria-describedby="tooltip" name="toggle-button">
                    <!-- Ellipses (row of three dots) -->
                    <svg width="12" height="12" viewBox="0 0 12 12" class="ellipses">
                        <ellipse cx="2" cy="6" rx="1" ry="1" fill="currentColor" />
                        <ellipse cx="6" cy="6" rx="1" ry="1" fill="currentColor" />
                        <ellipse cx="10" cy="6" rx="1" ry="1" fill="currentColor" />
                    </svg>

                    <!-- X (the symbol for closing) -->
                    <svg width="12" height="12" viewBox="0 0 12 12" class="cross hidden">
                        <line x1="1" y1="1" x2="11" y2="11" stroke="currentColor" stroke-width="1"/>
                        <line x1="11" y1="1" x2="1" y2="11" stroke="currentColor" stroke-width="1"/>
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

        // Prevent click events on the tooltip from bubbling up to higher-level HTML elements.
        this.tooltipWrapperEl = this.shadowRoot.querySelector(".tooltip-wrapper");
        this.tooltipWrapperEl.addEventListener("click", this.stopEventPropagation);
    }

    /**
     * Prevents the specified event from bubbling up to higher-level HTML elements.
     * 
     * @param {Event} event The event.
     */
    stopEventPropagation(event) {
        event.stopPropagation();
    }
    
    /**
     * Handle a change in the value (or the addition, removal, or replacement) of any HTML attribute
     * whose name is listed in the `observedAttributes` list.
     * 
     * @param {string} attributeName Name of the attribute whose value changed
     * @param {string | null} oldValue Value the attribute changed _from_
     * @param {string | null} newValue Value the attribute changed _to_
     * 
     * Reference: https://developer.mozilla.org/en-US/docs/Web/API/Web_components/Using_custom_elements#responding_to_attribute_changes
     */
    attributeChangedCallback(attributeName, oldValue, newValue) {
        // If the "is-open" attribute changed and its value is "true", show the "X" (Castlevania boomerang) icon.
        // Otherwise, show the "..." (Pac-Man bait) icon.
        if (attributeName === "is-open") {
            const ellipsesEl = this.shadowRoot.querySelector("button .ellipses");
            const crossEl = this.shadowRoot.querySelector("button .cross");
            if (typeof newValue === "string" && newValue.toLowerCase() === "true") {
                ellipsesEl.classList.add("hidden");
                crossEl.classList.remove("hidden");
            } else {
                crossEl.classList.add("hidden");
                ellipsesEl.classList.remove("hidden");
            }
        }
    }

    disconnectedCallback() {
        console.debug("Cleaning up event listener(s)");
        if (this.tooltipWrapperEl !== null) {
            this.tooltipWrapperEl.removeEventListener("click", this.stopEventPropagation);
        }
    }
}
