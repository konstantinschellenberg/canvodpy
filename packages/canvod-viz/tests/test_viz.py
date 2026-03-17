"""Tests for canvod-viz package."""

import pytest


def test_imports():
    """Test that all modules can be imported."""
    from canvod.viz import (
        HemisphereVisualizer,
        HemisphereVisualizer2D,
        HemisphereVisualizer3D,
        PlotStyle,
        PolarPlotStyle,
        create_interactive_style,
        create_publication_style,
    )

    assert HemisphereVisualizer is not None
    assert HemisphereVisualizer2D is not None
    assert HemisphereVisualizer3D is not None
    assert PolarPlotStyle is not None
    assert PlotStyle is not None
    assert create_interactive_style is not None
    assert create_publication_style is not None


def test_polar_plot_style_defaults():
    """Test PolarPlotStyle default values."""
    from canvod.viz import PolarPlotStyle

    style = PolarPlotStyle()

    assert style.cmap == "viridis"
    assert style.edgecolor == "black"
    assert style.linewidth == 0.5
    assert style.alpha == 1.0
    assert style.figsize == (10, 10)
    assert style.dpi == 100
    assert style.theta_labels == [0, 30, 60, 90]


def test_plot_style_defaults():
    """Test PlotStyle default values."""
    from canvod.viz import PlotStyle

    style = PlotStyle()

    assert style.colormap == "viridis"
    assert style.colorscale == "Viridis"
    assert style.background_color == "white"
    assert style.text_color == "black"
    assert style.font_size == 11
    assert style.dark_mode is False


def test_plot_style_to_polar_style():
    """Test conversion from PlotStyle to PolarPlotStyle."""
    from canvod.viz import PlotStyle

    plot_style = PlotStyle(colormap="plasma", edge_linewidth=0.3, dark_mode=True)

    polar_style = plot_style.to_polar_style()

    assert polar_style.cmap == "plasma"
    assert polar_style.linewidth == 0.3
    assert polar_style.edgecolor == "white"  # dark mode


def test_plot_style_to_plotly_layout():
    """Test conversion from PlotStyle to plotly layout."""
    from canvod.viz import PlotStyle

    # Light mode
    light_style = PlotStyle(dark_mode=False)
    light_layout = light_style.to_plotly_layout()

    assert light_layout["template"] == "plotly"
    assert light_layout["paper_bgcolor"] == "white"
    assert light_layout["font"]["color"] == "black"

    # Dark mode
    dark_style = PlotStyle(dark_mode=True)
    dark_layout = dark_style.to_plotly_layout()

    assert dark_layout["template"] == "plotly_dark"
    assert dark_layout["paper_bgcolor"] == "#111111"
    assert dark_layout["font"]["color"] == "white"


def test_create_publication_style():
    """Test publication style factory."""
    from canvod.viz import create_publication_style

    style = create_publication_style()

    assert style.dark_mode is False
    assert style.colormap == "viridis"
    assert style.font_size == 12
    assert style.title_size == 16
    assert style.edge_linewidth == 0.3


def test_create_interactive_style():
    """Test interactive style factory."""
    from canvod.viz import create_interactive_style

    # Dark mode
    dark = create_interactive_style(dark_mode=True)
    assert dark.dark_mode is True
    assert dark.colormap == "plasma"
    assert dark.background_color == "#111111"

    # Light mode
    light = create_interactive_style(dark_mode=False)
    assert light.dark_mode is False
    assert light.colormap == "viridis"
    assert light.background_color == "white"


def test_hemisphere_visualizer_2d_requires_grid():
    """Test that HemisphereVisualizer2D requires a grid."""
    from canvod.viz import HemisphereVisualizer2D

    # Should raise error without grid
    with pytest.raises(TypeError):
        HemisphereVisualizer2D()  # Missing grid parameter


def test_hemisphere_visualizer_3d_requires_grid():
    """Test that HemisphereVisualizer3D requires a grid."""
    from canvod.viz import HemisphereVisualizer3D

    # Should raise error without grid
    with pytest.raises(TypeError):
        HemisphereVisualizer3D()  # Missing grid parameter


def test_hemisphere_visualizer_requires_grid():
    """Test that HemisphereVisualizer requires a grid."""
    from canvod.viz import HemisphereVisualizer

    # Should raise error without grid
    with pytest.raises(TypeError):
        HemisphereVisualizer()  # Missing grid parameter


def test_visualizer_with_grid():
    """Test visualizer creation with an actual grid from canvod-grids."""
    from canvod.grids import create_hemigrid
    from canvod.viz import HemisphereVisualizer2D

    grid = create_hemigrid("equal_area", angular_resolution=30.0)
    viz = HemisphereVisualizer2D(grid)

    assert viz.grid is grid


def test_style_parameter_override():
    """Test that style parameters can be overridden."""
    from canvod.viz import PolarPlotStyle

    style = PolarPlotStyle(cmap="plasma", figsize=(12, 12), dpi=300, linewidth=0.2)

    assert style.cmap == "plasma"
    assert style.figsize == (12, 12)
    assert style.dpi == 300
    assert style.linewidth == 0.2
    # Defaults should still work
    assert style.alpha == 1.0
