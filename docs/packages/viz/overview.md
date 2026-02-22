# canvod-viz

## Purpose

The `canvod-viz` package provides 2D and 3D hemispheric visualization for GNSS-T grids, SNR data, and VOD results. It wraps matplotlib (publication-quality polar plots) and plotly (interactive 3D hemispheres) behind a unified API.

---

## Components

<div class="grid cards" markdown>

-   :fontawesome-solid-circle-half-stroke: &nbsp; **HemisphereVisualizer**

    ---

    Unified entry point combining both 2D and 3D backends.
    Swap between publication and interactive modes with one method call.

-   :fontawesome-solid-chart-area: &nbsp; **HemisphereVisualizer2D**

    ---

    Matplotlib polar projection plots. Patch-based rendering of grid cells.
    Publication-quality output at configurable DPI.

-   :fontawesome-solid-cube: &nbsp; **HemisphereVisualizer3D**

    ---

    Plotly interactive 3D hemispheres. Pan, zoom, rotate in the browser.
    One-call HTML export for sharing.

-   :fontawesome-solid-circle-dot: &nbsp; **Tissot Indicatrix**

    ---

    `add_tissot_indicatrix` — overlay angular distortion circles on 2D
    plots to evaluate grid cell shape fidelity.

</div>

---

## Usage

=== "Unified API (recommended)"

    ```python
    from canvod.grids import create_hemigrid
    from canvod.viz import HemisphereVisualizer

    grid = create_hemigrid("equal_area", angular_resolution=10.0)
    viz  = HemisphereVisualizer(grid)

    # Publication-quality 2D
    fig_2d, ax_2d = viz.plot_2d(data=vod_data, title="VOD Distribution")

    # Interactive 3D
    fig_3d = viz.plot_3d(data=vod_data, title="Interactive VOD")
    fig_3d.show()
    ```

=== "Convenience functions"

    ```python
    from canvod.viz import visualize_grid, visualize_grid_3d, add_tissot_indicatrix

    fig, ax = visualize_grid(grid, data=vod_data, cmap="viridis")
    add_tissot_indicatrix(ax, grid, n_sample=5)

    fig_3d = visualize_grid_3d(grid, data=vod_data)
    ```

=== "Publication output"

    ```python
    viz = HemisphereVisualizer(grid)
    viz.set_style(create_publication_style())

    fig, ax = viz.create_publication_figure(
        data=vod_data,
        title="VOD Distribution",
        save_path="figure_3.png",
        dpi=600,
    )
    ```

=== "Interactive export"

    ```python
    viz.set_style(create_interactive_style(dark_mode=True))

    fig = viz.create_interactive_explorer(
        data=vod_data,
        dark_mode=True,
        save_html="explorer.html",
    )
    ```

=== "Side-by-side comparison"

    ```python
    (fig_2d, ax_2d), fig_3d = viz.create_comparison_plot(data=vod_data)
    ```

---

## Styling

| Style factory | Use case |
| ------------- | -------- |
| `create_publication_style()` | Print-ready figures, configurable DPI, white background |
| `create_interactive_style()` | Browser-ready Plotly, dark mode option |

Both return a `PlotStyle` / `PolarPlotStyle` object passed to `viz.set_style()`.

---

## Dependencies

!!! info "Optional backends"

    - **matplotlib** — required for `HemisphereVisualizer2D` and all 2D functions
    - **plotly** — required for `HemisphereVisualizer3D` and all 3D functions
    - **canvod-grids** — always required for grid geometry

    Neither backend is a hard dependency of `canvod-viz` itself; import errors are
    raised only when the corresponding visualizer is instantiated.
